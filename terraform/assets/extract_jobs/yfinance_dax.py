import sys
import json
import boto3
import yfinance as yf
import pandas as pd
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType
)
from pyspark.sql.functions import lit, current_timestamp

# Initialize Glue context and Spark session
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'bronze_bucket',
    'ingest_date',
    'api_start_date',
    'api_end_date',
    'interval',
    'ticker_config_path'    # S3 path to ticker JSON config
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

BRONZE_BUCKET      = args['bronze_bucket']
INGEST_DATE        = args['ingest_date']
API_START_DATE     = args['api_start_date']
API_END_DATE       = args['api_end_date']   # exclusive
INTERVAL           = args['interval']
TICKER_CONFIG_PATH = args['ticker_config_path']

# Failure threshold: abort if too many tickers fail
# DAX 40 has quarterly changes, some failures are normal
FAILURE_THRESHOLD = 0.20

# Schema 
# Source-shaped schema: mirrors yfinance output exactly
# auto_adjust=False returns: Open, High, Low,
#                            Close, Adj Close, Volume
# No transformation - bronze preserves source truth
# date stays StringType  -> casting happens in silver
# volume is LongType     -> DAX daily volume > 2.1B possible
# adj_close included     -> source-provided adjusted close
#                          silver decides whether to use close or adj_close
DAX_SCHEMA = StructType([
    StructField("date",      StringType(), False),
    StructField("open",      DoubleType(), True),
    StructField("high",      DoubleType(), True),
    StructField("low",       DoubleType(), True),
    StructField("close",     DoubleType(), True),
    StructField("adj_close", DoubleType(), True),
    StructField("volume",    LongType(),   True),
    StructField("ticker",    StringType(), False),
])

def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Parse s3://bucket/key into bucket and key."""
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Expected S3 URI, got: {s3_uri}")

    path = s3_uri.replace("s3://", "", 1)
    bucket, _, key = path.partition("/")

    if not bucket or not key:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")

    return bucket, key

# Ticker config 
def load_tickers(s3_path: str) -> list:
    """
    Load ticker list from S3 JSON config file.
    Decouples ticker list from script code.
    Update S3 file when DAX composition changes
    without redeploying Glue job.

    Expected format:
    {
        "market": "DAX40",
        "exchange": "XETRA",
        "as_of": "2024-01-01",
        "source": "https://www.dax-indices.com",
        "tickers": ["SAP.DE", "SIE.DE", ...]
    }
    """
    bucket, key = parse_s3_uri(s3_path)

    s3       = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    config   = json.loads(response["Body"].read())

    tickers = config.get("tickers")
    if not tickers or not isinstance(tickers, list):
        raise ValueError("Ticker config must contain a non-empty 'tickers' list")
    print(f"Loaded {len(tickers)} tickers")
    print(f"Market:  {config.get('market', 'unknown')}")
    print(f"As of:   {config.get('as_of', 'unknown')}")
    return tickers

# Single ticker handler 
def download_single_ticker(
    ticker: str,
    start: str,
    end: str,
    interval: str
) -> pd.DataFrame:
    """
    Download a single ticker safely.
    yfinance group_by='ticker' behaves differently
    for single vs multiple tickers - this handles
    both cases consistently by always downloading
    one ticker at a time.
    """
    df = yf.download(
        ticker,
        start       = start,
        end         = end,
        interval    = interval,
        auto_adjust = False,   # preserve raw source prices
        progress    = False,
        threads     = False 
    )
    return df

# Function to fetch data from API
def fetch_raw(tickers: list) -> tuple:
    """
    Download raw OHLCV + Adj Close from yfinance.

    auto_adjust=False preserves unadjusted OHLC columns and includes
    source-provided Adj Close. Silver decides which price field to use.

    API_END_DATE is treated as exclusive by yfinance.
    Example: start=2024-01-01, end=2025-01-01
             fetches all trading days in 2024.

    Downloads each ticker individually to avoid
    MultiIndex shape issues with group_by='ticker'.

    Returns:
        records: list of row dicts ready for Spark
        failed:  list of (ticker, reason) tuples
    """
    records = []
    failed  = []

    for ticker in tickers:
        try:
            df = download_single_ticker(
                ticker,
                start    = API_START_DATE,
                end      = API_END_DATE,
                interval = INTERVAL
            )

            if df.empty:
                raise ValueError("empty DataFrame returned")

            # Normalize column names
            df = df.reset_index()

            df.columns = [
                str(c).lower().replace(" ", "_")
                for c in df.columns
            ]

            if "datetime" in df.columns and "date" not in df.columns:
                df = df.rename(columns={"datetime": "date"})

            if "index" in df.columns and "date" not in df.columns:
                df = df.rename(columns={"index": "date"})

            # Drop rows where core OHLC is null
            # Non-trading days can appear as null rows
            df = df.dropna(
                subset=["open", "high", "low", "close"],
                how="all"
            )

            if df.empty:
                raise ValueError(
                    "all rows null after dropna on OHLC"
                )

            df["date"]   = df["date"].astype(str)
            df["ticker"] = ticker

            # Volume is count-like, but yfinance/pandas may represent it as float
            # when missing values exist. Preserve missing volume as null in Bronze.
            df["volume"] = df["volume"].astype("Int64")

            # Convert pandas missing values such as NaN, NaT, and pd.NA to Python None,
            # so Spark can write them as nulls.
            df = df.astype(object).where(pd.notnull(df), None)

            records.extend(df.to_dict("records"))
            print(f"{ticker}: {len(df)} rows")

        except Exception as e:
            failed.append((ticker, str(e)))
            print(f"{ticker}: failed - {e}")

    return records, failed

# Audit failed tickers 
def write_failed_audit(
    failed: list,
    tickers: list
) -> None:
    """
    Write failed tickers to S3 as audit JSON.
    Airflow can check this file and alert on failures.
    Provides traceability without a full database table.
    """
    if not failed:
        return

    audit = [
        {
            "ticker": ticker,
            "reason": reason,
            "source": "yfinance",
            "market": "dax",
            "interval": INTERVAL,
            "api_start_date": API_START_DATE,
            "api_end_date": API_END_DATE,
            "ingest_date": INGEST_DATE,
            "total_tickers": len(tickers),
            "failed_count":  len(failed),
        }
        for ticker, reason in failed
    ]

    audit_path = (
        f"s3://{BRONZE_BUCKET}/audit/yfinance_prices/"
        f"market=dax/"
        f"interval={INTERVAL}/"
        f"ingest_date={INGEST_DATE}/"
        f"failed_tickers/"
    )

    # Write via Spark for consistency
    spark.createDataFrame(audit) \
        .withColumn("audited_at", current_timestamp()) \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .json(audit_path)

    print(f"Audit written: {audit_path}")

# Function to write DataFrames to S3
def write_bronze(records: list) -> None:
    s3_path = (
        f"s3://{BRONZE_BUCKET}/yfinance_prices/"
        f"market=dax/"
        f"interval={INTERVAL}/"
        f"ingest_date={INGEST_DATE}/"
    )
    
    # Convert data to Spark DataFrames
    spark.createDataFrame(records, schema=DAX_SCHEMA) \
         .withColumn("source",         lit("yfinance")) \
         .withColumn("market",         lit("dax")) \
         .withColumn("interval",       lit(INTERVAL)) \
         .withColumn("api_start_date", lit(API_START_DATE)) \
         .withColumn("api_end_date",   lit(API_END_DATE)) \
         .withColumn("ingest_date",    lit(INGEST_DATE)) \
         .withColumn("ingested_at",    current_timestamp()) \
         .write \
         .mode("overwrite") \
         .parquet(s3_path)

    print(f"DAX: written to {s3_path}")

# Main 
def main() -> None:
    print(f"Starting yfinance DAX prices extract")
    print(f"Interval:    {INTERVAL}")
    print(f"Date range:  {API_START_DATE} -> {API_END_DATE} (exclusive)")
    print(f"Ingest date: {INGEST_DATE}")

    # Load ticker config from S3
    tickers = load_tickers(TICKER_CONFIG_PATH)
    print(f"Tickers:     {len(tickers)}")

    # Fetch data
    records, failed = fetch_raw(tickers)

    # Write failed ticker audit
    write_failed_audit(failed, tickers)

    # Log failures
    if failed:
        print(f"\nFailed tickers ({len(failed)}):")
        for ticker, reason in failed:
            print(f"   {ticker}: {reason}")

    # Abort if failure rate exceeds threshold
    failure_rate = len(failed) / len(tickers)
    if failure_rate > FAILURE_THRESHOLD:
        raise RuntimeError(
            f"Failure rate {failure_rate:.0%} exceeds "
            f"threshold {FAILURE_THRESHOLD:.0%} — "
            f"{len(failed)}/{len(tickers)} tickers failed. "
            f"Aborting to prevent partial data in bronze."
        )

    if not records:
        raise ValueError(
            f"DAX: no records fetched for "
            f"{API_START_DATE} -> {API_END_DATE} "
            f"interval={INTERVAL}"
        )

    print(
        f"\nFetched {len(records)} total rows "
        f"from {len(tickers) - len(failed)} tickers"
    )

    write_bronze(records)

    print("\n✅ yfinance DAX prices extract complete")

main()
job.commit()
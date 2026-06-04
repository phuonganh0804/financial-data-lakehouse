import sys
import time
import requests
import pandas as pd
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType
)
from pyspark.sql.functions import lit, current_timestamp

# Initialize Glue context and Spark session
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'bronze_bucket',
    'ingest_date',
    'api_start_date',
    'api_end_date',
    'interval'
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

BRONZE_BUCKET  = args['bronze_bucket']
INGEST_DATE    = args['ingest_date']
API_START_DATE = args['api_start_date']
API_END_DATE   = args['api_end_date']   # exclusive
INTERVAL       = args['interval']
SYMBOLS        = ['BTCUSDT', 'ETHUSDT']
MAX_LIMIT      = 1000
BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"

MAX_RETRIES = 3
RETRY_DELAY = 15
RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}

# Define Schema
# Raw Binance kline schema — no casting, values as returned
# Numbers stay StringType  -> Binance returns "50000.00"
# Timestamps stay LongType -> milliseconds epoch
# trade_count is LongType  -> can exceed IntegerType on high-volume symbols
# Casting to proper types happens in silver layer
BINANCE_SCHEMA = StructType([
    StructField("open_time",              LongType(),   False),
    StructField("open",                   StringType(), False),
    StructField("high",                   StringType(), False),
    StructField("low",                    StringType(), False),
    StructField("close",                  StringType(), False),
    StructField("volume",                 StringType(), False),
    StructField("close_time",             LongType(),   True),
    StructField("quote_volume",           StringType(), True),
    StructField("trade_count",            LongType(),   True),
    StructField("taker_buy_base_volume",  StringType(), True),
    StructField("taker_buy_quote_volume", StringType(), True),
    StructField("ignore",                 StringType(), True),
])

def to_epoch_ms(date_str: str) -> int:
    return int(pd.Timestamp(date_str, tz="UTC").timestamp() * 1000)


def fetch_page(
    session: requests.Session,
    symbol: str,
    params: dict,
) -> list:
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(BINANCE_KLINES_URL, params=params, timeout=30)

            if response.status_code in RETRYABLE_HTTP_STATUS:
                raise RuntimeError(
                    f"temporary HTTP {response.status_code}: "
                    f"{response.text[:200]}"
                )

            response.raise_for_status()
            return response.json()

        except (RuntimeError, requests.exceptions.RequestException) as e:
            last_error = e

            if attempt == MAX_RETRIES:
                break

            sleep_seconds = RETRY_DELAY * attempt
            print(
                f"{symbol}: attempt {attempt}/{MAX_RETRIES} failed: {e}. "
                f"Retrying in {sleep_seconds}s..."
            )
            time.sleep(sleep_seconds)

    raise RuntimeError(
        f"{symbol}: page fetch failed after {MAX_RETRIES} attempts - {last_error}"
    )


def fetch_raw(session: requests.Session, symbol: str) -> list:
    start_ms   = to_epoch_ms(API_START_DATE)
    end_ms     = to_epoch_ms(API_END_DATE)
    all_data   = []
    current_ms = start_ms

    while current_ms < end_ms:
        params = {
            "symbol":    symbol,
            "interval":  INTERVAL,
            "startTime": current_ms,
            "endTime":   end_ms,
            "limit":     MAX_LIMIT,
        }

        batch = fetch_page(session, symbol, params)

        if not batch:
            break

        for row in batch:
            if len(row) != 12:
                raise ValueError(
                    f"{symbol}: expected 12 fields per kline, "
                    f"got {len(row)} — Binance API may have changed"
                )

        all_data.extend(batch)
        print(f"  {symbol}: fetched {len(all_data)} rows so far...")

        current_ms = batch[-1][6] + 1

        if len(batch) < MAX_LIMIT:
            break

    if not all_data:
        raise ValueError(
            f"{symbol}: empty response for "
            f"{API_START_DATE} -> {API_END_DATE} "
            f"interval={INTERVAL}"
        )

    print(f"{symbol}: {len(all_data)} total rows fetched")
    return all_data

# Function to write DataFrames to S3
def write_bronze(data: list, symbol: str) -> None:
    s3_path = (
        f"s3://{BRONZE_BUCKET}/binance_klines/"
        f"symbol={symbol}/"
        f"interval={INTERVAL}/"
        f"ingest_date={INGEST_DATE}/"
    )
    # Convert data to Spark DataFrames
    spark.createDataFrame(data, schema=BINANCE_SCHEMA) \
         .withColumn("source",         lit("binance")) \
         .withColumn("symbol",         lit(symbol)) \
         .withColumn("interval",       lit(INTERVAL)) \
         .withColumn("api_start_date", lit(API_START_DATE)) \
         .withColumn("api_end_date",   lit(API_END_DATE)) \
         .withColumn("ingest_date",    lit(INGEST_DATE)) \
         .withColumn("ingested_at",    current_timestamp()) \
         .write \
         .mode("overwrite") \
         .parquet(s3_path)

    print(f"✅ {symbol}: written to {s3_path}")

# Main 
def main() -> None:
    print(f"Starting Binance klines extract")
    print(f"Interval:    {INTERVAL}")
    print(f"Date range:  {API_START_DATE} -> {API_END_DATE} (exclusive)")
    print(f"Ingest date: {INGEST_DATE}")
    print(f"Symbols:     {SYMBOLS}")

    with requests.Session() as session:
        for symbol in SYMBOLS:
            print(f"\nProcessing {symbol}...")
            data = fetch_raw(session, symbol)
            write_bronze(data, symbol)

    print("\n✅ Binance klines extract complete")

main()
job.commit()
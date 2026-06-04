import sys
import json
import time
import uuid
from datetime import datetime, timezone

import boto3
import requests
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructField, StructType, StringType


args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "bronze_bucket",
    "ingest_date",
    "api_start_date",
    "api_end_date",
    "interval",
    "ticker_config_path",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BRONZE_BUCKET = args["bronze_bucket"]
INGEST_DATE = args["ingest_date"]
API_START_DATE = args["api_start_date"]
API_END_DATE = args["api_end_date"]
TICKER_CONFIG_PATH = args["ticker_config_path"]

SSM_PARAMETER = "/financial-data-lakehouse/twelvedata-api-key"
TWELVEDATA_URL = "https://api.twelvedata.com/time_series"

INTERVAL_MAP = {"1d": "1day", "1w": "1week", "1mo": "1month"}
INTERVAL = INTERVAL_MAP.get(args["interval"], args["interval"])

MAX_RETRIES = 3
REQUEST_DELAY = 8
RETRY_DELAY = 20
MAX_OUTPUT_SIZE = 5000
FAILURE_THRESHOLD = 0

RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}
RETRYABLE_API_MESSAGES = ("rate limit", "too many requests")

# Schema 
# Source-shaped schema — mirrors Twelvedata output exactly
# All values returned as strings by Twelvedata API
# No adj_close available on free tier
# -> noted as known limitation, silver decides
# Casting to proper types happens in silver layer
EQUITY_SCHEMA = StructType([
    StructField("datetime", StringType(), False),
    StructField("open", StringType(), True),
    StructField("high", StringType(), True),
    StructField("low", StringType(), True),
    StructField("close", StringType(), True),
    StructField("volume", StringType(), True),
    StructField("symbol", StringType(), False),
    StructField("currency", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("exchange_timezone", StringType(), True),
    StructField("mic_code", StringType(), True),
    StructField("instrument_type", StringType(), True),
])

def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Expected S3 URI, got: {s3_uri}")

    path = s3_uri.replace("s3://", "", 1)
    bucket, _, key = path.partition("/")

    if not bucket or not key:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")

    return bucket, key


def load_ticker_config(s3_uri: str) -> dict:
    bucket, key = parse_s3_uri(s3_uri)
    body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    config = json.loads(body)

    required = ["market", "exchange", "symbols"]
    missing = [field for field in required if not config.get(field)]
    if missing:
        raise ValueError(f"Ticker config missing required fields: {missing}")

    if not isinstance(config["symbols"], list):
        raise ValueError("Ticker config field 'symbols' must be a list")
    
    if not config["symbols"]:
        raise ValueError("Ticker config field 'symbols' must be non-empty")

    if not all(isinstance(symbol, str) for symbol in config["symbols"]):
        raise ValueError("All symbols must be strings")

    if len(config["symbols"]) != len(set(config["symbols"])):
        raise ValueError("Ticker config contains duplicate symbols")
    return config


def get_api_key() -> str:
    ssm = boto3.client("ssm", region_name="eu-central-1")
    return ssm.get_parameter(
        Name=SSM_PARAMETER,
        WithDecryption=True,
    )["Parameter"]["Value"]


def should_retry_api_error(message: str) -> bool:
    text = message.lower()
    return any(token in text for token in RETRYABLE_API_MESSAGES)


def sleep_before_retry(symbol: str, attempt: int, error: Exception) -> None:
    sleep_seconds = RETRY_DELAY * attempt
    print(
        f"{symbol}: attempt {attempt}/{MAX_RETRIES} failed: {error}. "
        f"Retrying in {sleep_seconds}s..."
    )
    time.sleep(sleep_seconds)

def fetch_symbol(
    session: requests.Session,
    symbol: str,
    api_key: str,
    exchange: str,
) -> tuple[list, dict]:
    params = {
        "symbol": symbol,
        "exchange": exchange,
        "interval": INTERVAL,
        "adjust": "none",
        "start_date": API_START_DATE,
        "end_date": API_END_DATE,
        "outputsize": MAX_OUTPUT_SIZE,
        "apikey": api_key,
    }

    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(TWELVEDATA_URL, params=params, timeout=30)

            if response.status_code in RETRYABLE_HTTP_STATUS:
                raise RuntimeError(
                    f"temporary HTTP {response.status_code}: "
                    f"{response.text[:200]}"
                )

            if response.status_code >= 400:
                raise ValueError(
                    f"{symbol}: non-retryable HTTP {response.status_code}: "
                    f"{response.text[:200]}"
                )

            data = response.json()

            if data.get("status") == "error":
                message = data.get("message", "unknown error")

                if should_retry_api_error(message):
                    raise RuntimeError(f"retryable API error: {message}")

                raise ValueError(f"{symbol}: API error - {message}")

            values = data.get("values")
            if not values:
                raise ValueError(
                    f"{symbol}: empty response for "
                    f"{API_START_DATE} -> {API_END_DATE}"
                )

            print(f"{symbol}: {len(values)} rows fetched")
            return values, data.get("meta", {})

        except (RuntimeError, requests.exceptions.RequestException) as e:
            last_error = e

            if attempt == MAX_RETRIES:
                break

            sleep_before_retry(symbol, attempt, e)

    raise RuntimeError(
        f"{symbol}: failed after {MAX_RETRIES} attempts - {last_error}"
    )

# Fetch all symbols
def fetch_raw(config: dict, api_key: str) -> tuple[list, list]:
    records = []
    failed = []

    symbols = config["symbols"]
    exchange = config["exchange"]

    with requests.Session() as session:
        for index, symbol in enumerate(symbols):
            try:
                values, meta = fetch_symbol(session, symbol, api_key, exchange)

                for row in values:
                    records.append({
                        "datetime": row["datetime"],
                        "open": row.get("open"),
                        "high": row.get("high"),
                        "low": row.get("low"),
                        "close": row.get("close"),
                        "volume": row.get("volume"),
                        "symbol": symbol,
                        "currency": meta.get("currency"),
                        "exchange": meta.get("exchange", exchange),
                        "exchange_timezone": meta.get("exchange_timezone"),
                        "mic_code": meta.get("mic_code"),
                        "instrument_type": meta.get("type"),
                    })

            except Exception as e:
                failed.append((symbol, str(e)))
                print(f"{symbol}: failed - {e}")

            if index < len(symbols) - 1:
                time.sleep(REQUEST_DELAY)

    return records, failed

# Write failed audit
def write_failed_audit(failed: list, config: dict) -> None:
    """
    Write failed Twelve Data symbols to S3 as append-only audit records.
    Each run writes to a unique path, so no S3 delete permission is needed.
    """
    if not failed:
        print("No Twelve Data failures detected. Skipping audit write.")
        return

    market = config["market"]
    exchange = config["exchange"]
    universe = config.get("universe", "unknown")

    run_id = (
        f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_"
        f"{uuid.uuid4().hex[:8]}"
    )

    audit_path = (
        f"s3://{BRONZE_BUCKET}/audit/equity_prices/"
        f"provider=twelvedata/"
        f"market={market}/"
        f"exchange={exchange}/"
        f"interval={INTERVAL}/"
        f"ingest_date={INGEST_DATE}/"
        f"audit_type=failed_symbols/"
        f"run_id={run_id}/"
    )

    audit_records = [
        {
            "symbol": symbol,
            "reason": reason,
            "source": "twelvedata",
            "market": market,
            "exchange": exchange,
            "universe": universe,
            "interval": INTERVAL,
            "api_start_date": API_START_DATE,
            "api_end_date": API_END_DATE,
            "ingest_date": INGEST_DATE,
        }
        for symbol, reason in failed
    ]

    (
        spark.createDataFrame(audit_records)
        .withColumn("run_id", lit(run_id))
        .withColumn("audited_at", current_timestamp())
        .coalesce(1)
        .write
        .mode("errorifexists")
        .json(audit_path)
    )

    print(f"Audit written: {audit_path}")

# Function to write DataFrames to S3
def write_bronze(records: list, config: dict) -> None:
    market = config["market"]
    exchange = config["exchange"]

    s3_path = (
        f"s3://{BRONZE_BUCKET}/equity_prices/"
        f"provider=twelvedata/"
        f"market={market}/"
        f"exchange={exchange}/"
        f"interval={INTERVAL}/"
        f"ingest_date={INGEST_DATE}/"
    )

    spark.createDataFrame(records, schema=EQUITY_SCHEMA) \
        .withColumn("source", lit("twelvedata")) \
        .withColumn("market", lit(market)) \
        .withColumn("interval", lit(INTERVAL)) \
        .withColumn("api_start_date", lit(API_START_DATE)) \
        .withColumn("api_end_date", lit(API_END_DATE)) \
        .withColumn("ingest_date", lit(INGEST_DATE)) \
        .withColumn("ingested_at", current_timestamp()) \
        .write \
        .mode("overwrite") \
        .parquet(s3_path)

    print(f"Equities written to {s3_path}")

# Main 
def main() -> None:
    print("Starting Twelve Data equities extract")
    print(f"Interval:    {INTERVAL}")
    print(f"Date range:  {API_START_DATE} -> {API_END_DATE} (exclusive)")
    print(f"Ingest date: {INGEST_DATE}")
    print(f"Config path: {TICKER_CONFIG_PATH}")

    config = load_ticker_config(TICKER_CONFIG_PATH)
    symbols = config["symbols"]
    market = config["market"]
    exchange = config["exchange"]
    universe = config.get("universe", "unknown")

    print(f"Market:      {market}")
    print(f"Exchange:    {exchange}")
    print(f"Universe:    {universe}")
    print(f"Symbols:     {len(symbols)}")

    api_key = get_api_key()
    print("API key retrieved from SSM")

    records, failed = fetch_raw(config, api_key)

    write_failed_audit(failed, config)

    if failed:
        print(f"Failed symbols ({len(failed)}):")
        for symbol, reason in failed:
            print(f"{symbol}: {reason}")

    failure_rate = len(failed) / len(symbols)

    if failure_rate > FAILURE_THRESHOLD:
        raise RuntimeError(
            f"Failure rate {failure_rate:.0%} exceeds "
            f"threshold {FAILURE_THRESHOLD:.0%} - "
            f"{len(failed)}/{len(symbols)} symbols failed. "
            f"Aborting to prevent partial data in bronze."
        )

    if not records:
        raise ValueError(
            f"Equities: no records fetched for "
            f"{API_START_DATE} -> {API_END_DATE}, "
            f"market={market}, exchange={exchange}, interval={INTERVAL}"
        )

    print(
        f"Fetched {len(records)} total rows "
        f"from {len(symbols) - len(failed)} symbols"
    )

    write_bronze(records, config)

    print("Twelve Data equities extract complete")

main()
job.commit()
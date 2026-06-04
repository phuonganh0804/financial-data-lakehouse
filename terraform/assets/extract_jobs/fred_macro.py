import sys
import time
import uuid
import boto3
import requests
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType
)
from pyspark.sql.functions import lit, current_timestamp

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'bronze_bucket',
    'ingest_date',
    'api_start_date',
    'api_end_date'
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

BRONZE_BUCKET  = args['bronze_bucket']
INGEST_DATE    = args['ingest_date']
API_START_DATE = args['api_start_date']
API_END_DATE   = args['api_end_date']
SSM_PARAMETER  = '/financial-data-lakehouse/fred-api-key'
MAX_FAILED_SERIES = 0

FRED_URL = "https://api.stlouisfed.org/fred/series/observations"
MAX_RETRIES = 3
RETRY_DELAY = 20
RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}

# Macro series config
# Each series has its own frequency (daily, monthly, quarterly)
# Frequency is preserved as-is - no resampling in bronze
# Silver layer handles forward-fill and resampling
MACRO_SERIES = {
    "DFF": {
        "name": "Effective Federal Funds Rate",
        "frequency": "daily",
        "unit": "percent"
    },
    "CPIAUCSL": {
        "name": "US Consumer Price Index",
        "frequency": "monthly",
        "unit": "index_1982_1984_100"
    },
    "GDPC1": {
        "name": "US Real GDP",
        "frequency": "quarterly",
        "unit": "billions_chained_2017_usd"
    },
    "DGS10": {
        "name": "US 10Y Treasury Yield",
        "frequency": "daily",
        "unit": "percent"
    },
    "T10YIE": {
        "name": "US 10Y Inflation Expectations",
        "frequency": "daily",
        "unit": "percent"
    },
}

# Schema
# Source-shaped schema — mirrors FRED output exactly
# date stays StringType     -> casting happens in silver
# value is DoubleType       -> selected FRED series are numeric;
#                              missing values ("." in FRED) are dropped in bronze
# frequency + unit included -> important bronze metadata
#                              tells silver how to resample
# No interval column        -> FRED series have mixed frequencies
#                              frequency column captures this instead
FRED_SCHEMA = StructType([
    StructField("date",        StringType(), False),
    StructField("value",       DoubleType(), False),
    StructField("series_id",   StringType(), False),
    StructField("series_name", StringType(), False),
    StructField("frequency",   StringType(), False),
    StructField("unit",        StringType(), False),
])


def get_api_key(parameter_name: str) -> str:
    ssm = boto3.client('ssm', region_name='eu-central-1')
    return ssm.get_parameter(
        Name=parameter_name,
        WithDecryption=True
    )['Parameter']['Value']


def fetch_series(
    session: requests.Session,
    series_id: str,
    api_key: str,
) -> list:
    params = {
        "series_id":         series_id,
        "observation_start": API_START_DATE,
        "observation_end":   API_END_DATE,
        "api_key":           api_key,
        "file_type":         "json",
    }

    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(FRED_URL, params=params, timeout=30)

            if response.status_code in RETRYABLE_HTTP_STATUS:
                raise RuntimeError(
                    f"temporary HTTP {response.status_code}: "
                    f"{response.text[:200]}"
                )

            response.raise_for_status()
            data = response.json()

            # FRED uses "." to represent missing observations — drop them
            return [
                obs for obs in data.get("observations", [])
                if obs["value"] != "."
            ]

        except (RuntimeError, requests.exceptions.RequestException) as e:
            last_error = e

            if attempt == MAX_RETRIES:
                break

            sleep_seconds = RETRY_DELAY * attempt
            print(
                f"{series_id}: attempt {attempt}/{MAX_RETRIES} failed: {e}. "
                f"Retrying in {sleep_seconds}s..."
            )
            time.sleep(sleep_seconds)

    raise RuntimeError(
        f"{series_id}: failed after {MAX_RETRIES} attempts - {last_error}"
    )


def fetch_raw(api_key: str) -> tuple:
    records = []
    failed  = []

    with requests.Session() as session:
        for series_id, meta in MACRO_SERIES.items():
            try:
                observations = fetch_series(session, series_id, api_key)

                if not observations:
                    raise ValueError(
                        f"empty series for "
                        f"{API_START_DATE} -> {API_END_DATE}"
                    )

                for obs in observations:
                    records.append({
                        "date":        obs["date"],
                        "value":       float(obs["value"]),
                        "series_id":   series_id,
                        "series_name": meta["name"],
                        "frequency":   meta["frequency"],
                        "unit":        meta["unit"],
                    })

                print(
                    f"{meta['name']} "
                    f"({meta['frequency']}): "
                    f"{len(observations)} observations"
                )

            except Exception as e:
                failed.append((series_id, meta, str(e)))
                print(f"{series_id}: failed — {e}")

    return records, failed


def write_failed_audit(failed: list) -> None:
    if not failed:
        print("No FRED failures detected. Skipping audit write.")
        return

    run_id = (
        f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_"
        f"{uuid.uuid4().hex[:8]}"
    )

    audit_path = (
        f"s3://{BRONZE_BUCKET}/audit/fred_macro/"
        f"ingest_date={INGEST_DATE}/"
        f"audit_type=failed_series/"
        f"run_id={run_id}/"
    )

    audit_records = [
        {
            "series_id":      series_id,
            "reason":         reason,
            "series_name":    meta["name"],
            "frequency":      meta["frequency"],
            "unit":           meta["unit"],
            "source":         "fred",
            "api_start_date": API_START_DATE,
            "api_end_date":   API_END_DATE,
            "ingest_date":    INGEST_DATE,
        }
        for series_id, meta, reason in failed
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


def write_bronze(records: list) -> None:
    s3_path = (
        f"s3://{BRONZE_BUCKET}/fred_macro/"
        f"ingest_date={INGEST_DATE}/"
    )

    spark.createDataFrame(records, schema=FRED_SCHEMA) \
         .withColumn("source",         lit("fred")) \
         .withColumn("api_start_date", lit(API_START_DATE)) \
         .withColumn("api_end_date",   lit(API_END_DATE)) \
         .withColumn("ingest_date",    lit(INGEST_DATE)) \
         .withColumn("ingested_at",    current_timestamp()) \
         .write \
         .mode("overwrite") \
         .parquet(s3_path)

    print(f"FRED: written to {s3_path}")


def main() -> None:
    print(f"Starting FRED macro extract")
    print(f"Date range:  {API_START_DATE} -> {API_END_DATE}")
    print(f"Ingest date: {INGEST_DATE}")
    print(f"Series:      {len(MACRO_SERIES)}")
    print(f"Max failed series allowed: {MAX_FAILED_SERIES}")

    api_key = get_api_key(SSM_PARAMETER)
    print("API key retrieved from SSM")

    records, failed = fetch_raw(api_key)

    write_failed_audit(failed)

    if failed:
        print(f"\nFailed series ({len(failed)}):")
        for series_id, meta, reason in failed:
            print(f"  {series_id} ({meta['frequency']}): {reason}")

    if len(failed) > MAX_FAILED_SERIES:
        raise RuntimeError(
            f"{len(failed)}/{len(MACRO_SERIES)} FRED series failed — "
            f"aborting to prevent incomplete macro data. "
            f"Failed: {[s for s, _, _ in failed]}"
        )

    if not records:
        raise ValueError(
            f"FRED: no records fetched for "
            f"{API_START_DATE} -> {API_END_DATE}"
        )

    print(
        f"\nFetched {len(records)} total records "
        f"from {len(MACRO_SERIES) - len(failed)} series"
    )

    write_bronze(records)

    print("\nFRED macro extract complete")


main()
job.commit()

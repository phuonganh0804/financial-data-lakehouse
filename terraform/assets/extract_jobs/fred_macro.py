import sys
import boto3
from fredapi import Fred
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType
)
from pyspark.sql.functions import lit, current_timestamp

# Initialize Glue context and Spark session
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
SSM_PARAMETER  = '/dax-crypto-pipeline/fred-api-key'
MAX_FAILED_SERIES = 1

# Macro series config
# Each series has its own frequency (daily, monthly, quarterly)
# Frequency is preserved as-is - no resampling in bronze
# Silver layer handles forward-fill and resampling
MACRO_SERIES = {
    'ECBDFR': {
        'name':      'ECB Deposit Rate',
        'frequency': 'daily',
        'unit':      'percent'
    },
    'CP0000EZ19M086NEST': {
        'name':      'Euro Area Inflation HICP',
        'frequency': 'monthly',
        'unit':      'index_2015_100'
    },
    'CLVMEURSCAB1GQEA19': {
        'name':      'Euro Area GDP Growth',
        'frequency': 'quarterly',
        'unit':      'millions_eur'
    },
    'T10YIE': {
        'name':      'US 10Y Inflation Expectations',
        'frequency': 'daily',
        'unit':      'percent'
    },
}

# Schema
# Source-shaped schema — mirrors FRED output exactly
# date stays StringType     -> casting happens in silver
# value is DoubleType       -> selected FRED series are numeric;
#                              missing values are dropped in Bronze
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

# SSM 
def get_api_key(parameter_name: str) -> str:
    """Retrieve FRED API key from SSM Parameter Store"""
    ssm = boto3.client('ssm', region_name='eu-central-1')
    return ssm.get_parameter(
        Name=parameter_name,
        WithDecryption=True
    )['Parameter']['Value']

# Function to fetch data from API
def fetch_raw(api_key: str) -> tuple:
    """
    Fetch raw macro series from FRED API.

    Each series preserved at its native frequency:
    - Daily:     ECB rate, US 10Y expectations
    - Monthly:   Euro Area inflation
    - Quarterly: Euro Area GDP

    No resampling or forward-fill in bronze.
    Silver layer handles frequency alignment.

    Returns:
        records: list of row dicts ready for Spark
        failed:  list of (series_id, meta, reason) tuples
    """
    fred    = Fred(api_key=api_key)
    records = []
    failed  = []

    for series_id, meta in MACRO_SERIES.items():
        try:
            data = fred.get_series(
                series_id,
                observation_start=API_START_DATE,
                observation_end=API_END_DATE
            ).dropna()

            if data.empty:
                raise ValueError(
                    f"empty series for "
                    f"{API_START_DATE} -> {API_END_DATE}"
                )

            for date, value in data.items():
                records.append({
                    "date":        str(date.date()),
                    "value":       float(value),
                    "series_id":   series_id,
                    "series_name": meta['name'],
                    "frequency":   meta['frequency'],
                    "unit":        meta['unit'],
                })

            print(
                f"{meta['name']} "
                f"({meta['frequency']}): "
                f"{len(data)} observations"
            )

        except Exception as e:
            failed.append((series_id, meta, str(e)))
            print(f"{series_id}: failed — {e}")

    return records, failed

# Write failed audit 
def write_failed_audit(failed: list) -> None:
    """
    Write failed series to S3 as audit file.
    Consistent pattern with yfinance audit.
    """
    if not failed:
        return

    audit_path = (
        f"s3://{BRONZE_BUCKET}/audit/fred_macro/"
        f"ingest_date={INGEST_DATE}/"
        f"failed_series/"
    )

    audit_records = [
        {
            "series_id": series_id,
            "reason": reason,
            "series_name": meta['name'],
            "frequency":   meta['frequency'],
            "unit":        meta['unit'],
            "source": "fred",
            "api_start_date": API_START_DATE,
            "api_end_date": API_END_DATE,
            "ingest_date": INGEST_DATE,
        }
        for series_id, meta, reason in failed
    ]

    spark.createDataFrame(audit_records) \
        .withColumn("audited_at", current_timestamp()) \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .json(audit_path)

    print(f"Audit written: {audit_path}")

# Function to write DataFrames to S3
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

    print(f"✅ FRED: written to {s3_path}")

# Main 
def main() -> None:
    print(f"Starting FRED macro extract")
    print(f"Date range:  {API_START_DATE} -> {API_END_DATE}")
    print(f"Ingest date: {INGEST_DATE}")
    print(f"Series:      {len(MACRO_SERIES)}")
    print(f"Max failed series allowed: {MAX_FAILED_SERIES}")

    api_key = get_api_key(SSM_PARAMETER)
    print("API key retrieved from SSM")

    records, failed = fetch_raw(api_key)

    # Write audit for failed series
    write_failed_audit(failed)

    # Log failures
    if failed:
        print(f"\n Failed series ({len(failed)}):")
        for series_id, meta, reason in failed:
            print(f"   {series_id} ({meta['frequency']}): {reason}")

    # FRED has only 4 series — any failure is significant
    # Abort if more than 1 series fails
    if len(failed) > MAX_FAILED_SERIES:
        raise RuntimeError(
            f"{len(failed)}/4 FRED series failed — "
            f"aborting to prevent incomplete macro data. "
            f"Failed: {[s for s, _, _ in failed]}"
        )

    if not records:
        raise ValueError(
            f"FRED: no records fetched for "
            f"{API_START_DATE} -> {API_END_DATE}"
        )

    print(f"\nFetched {len(records)} total records "
          f"from {len(MACRO_SERIES) - len(failed)} series")

    write_bronze(records)

    print("\nFRED macro extract complete")

main()
job.commit()
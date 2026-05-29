import sys
import boto3
import pandas as pd
from fredapi import Fred
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# ── Glue job setup ────────────────────────────────
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'bronze_bucket',
    'ingest_date'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ── Config ────────────────────────────────────────
BRONZE_BUCKET   = args['bronze_bucket']
INGEST_DATE     = args['ingest_date']
SSM_PARAMETER   = '/dax-crypto-pipeline/fred-api-key'

MACRO_SERIES = {
    'ECBDFR':             'ECB Deposit Rate',
    'CP0000EZ19M086NEST': 'Euro Area Inflation HICP',
    'CLVMEURSCAB1GQEA19': 'Euro Area GDP Growth',
    'T10YIE':             'US 10Y Inflation Expectations',
}

# ── Get API key from SSM ──────────────────────────
def get_fred_api_key(parameter_name: str) -> str:
    ssm = boto3.client('ssm', region_name='eu-central-1')
    response = ssm.get_parameter(
        Name=parameter_name,
        WithDecryption=True
    )
    return response['Parameter']['Value']

# ── Fetch from FRED ───────────────────────────────
def fetch_macro(api_key: str) -> pd.DataFrame:
    fred   = Fred(api_key=api_key)
    frames = []

    for series_id, name in MACRO_SERIES.items():
        try:
            data = fred.get_series(
                series_id,
                observation_start='2023-01-01'
            )
            df = data.reset_index()
            df.columns       = ['date', 'value']
            df['series_id']   = series_id
            df['series_name'] = name
            df = df.dropna()
            frames.append(df)
            print(f"✅ {name}: {len(df)} observations")
        except Exception as e:
            print(f"❌ {name}: {e}")

    combined = pd.concat(frames, ignore_index=True)

    # Add metadata
    combined['source']      = 'fred'
    combined['ingested_at'] = datetime.now(timezone.utc).isoformat()

    return combined

# ── Quality checks ────────────────────────────────
def validate(df: pd.DataFrame) -> None:
    assert df.isnull().sum().sum() == 0, "FRED: nulls found"

    series_count = df['series_id'].nunique()
    assert series_count == len(MACRO_SERIES), \
        f"FRED: expected {len(MACRO_SERIES)} series, got {series_count}"

    print(f"✅ FRED: {series_count} series, quality checks passed")

# ── Write to S3 bronze ────────────────────────────
def write_to_bronze(df: pd.DataFrame) -> None:
    spark_df = spark.createDataFrame(df.astype(str))

    date    = INGEST_DATE
    s3_path = (
        f"s3://{BRONZE_BUCKET}/macro/fred/"
        f"year={date[:4]}/month={date[5:7]}/day={date[8:10]}/"
    )

    spark_df.write \
        .mode("overwrite") \
        .parquet(s3_path)

    print(f"✅ FRED: written to {s3_path}")

# ── Main ──────────────────────────────────────────
def main() -> None:
    print(f"Starting FRED macro extract job")
    print(f"Ingest date: {INGEST_DATE}")

    # Get API key securely from SSM
    fred_api_key = get_fred_api_key(SSM_PARAMETER)
    print("✅ API key retrieved from SSM")

    # Fetch
    df = fetch_macro(fred_api_key)
    print(f"Fetched {len(df)} total rows")

    # Validate
    validate(df)

    # Write
    write_to_bronze(df)

    print("\n✅ FRED macro extract job complete")

main()
job.commit()
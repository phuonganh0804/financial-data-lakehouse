import json
import sys

import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# Glue Spark job — structures the raw FRED responses from the immutable
# landing zone into columnar Parquet in the bronze layer.
#
# Landing stores the full observations payload verbatim, one file per series
# (including "." missing markers). Here we explode `observations`, drop "."
# rows, cast value to double, and attach the reference metadata
# (name/frequency/unit) that FRED's observations endpoint does not return.
# Bronze is rebuilt from landing, never the API.

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "landing_bucket",
    "bronze_bucket",
    "ingest_date",
    "api_start_date",
    "api_end_date",
    "macro_series_config_path",
])

LANDING_BUCKET           = args["landing_bucket"]
BRONZE_BUCKET            = args["bronze_bucket"]
INGEST_DATE              = args["ingest_date"]
API_START_DATE           = args["api_start_date"]
API_END_DATE             = args["api_end_date"]
MACRO_SERIES_CONFIG_PATH = args["macro_series_config_path"]

LANDING_PATH = f"s3://{LANDING_BUCKET}/fred_macro/"
BRONZE_PATH  = f"s3://{BRONZE_BUCKET}/fred_macro/"

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
# Keep partition columns as strings instead of letting Spark infer DateType
# from "ingest_date=2024-01-01" dir names — matches how we filter and write.
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

# Final bronze column order — mirrors the original source-shaped schema.
BRONZE_COLS = [
    "date", "value", "series_id", "series_name", "frequency", "unit",
    "source", "api_start_date", "api_end_date", "ingest_date", "ingested_at",
]


def parse_s3_uri(s3_uri: str) -> tuple:
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Expected S3 URI, got: {s3_uri}")
    path = s3_uri.replace("s3://", "", 1)
    bucket, _, key = path.partition("/")
    if not bucket or not key:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    return bucket, key


def series_metadata_df():
    """Reference metadata (name/frequency/unit), loaded from the shared S3
    config — the single source also used by the landing job. FRED's
    observations endpoint does not return these, so bronze attaches them."""
    bucket, key = parse_s3_uri(MACRO_SERIES_CONFIG_PATH)
    body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    series = json.loads(body).get("series", [])
    rows = [
        (item["series_id"], item["name"], item["frequency"], item["unit"])
        for item in series
    ]
    return spark.createDataFrame(
        rows, ["series_id", "series_name", "frequency", "unit"]
    )


def read_landing():
    # Each landing file is one full FRED response object, so read multiLine.
    # series_id / ingest_date / run_id come from the Hive-partitioned path.
    raw = (
        spark.read
        .option("multiLine", "true")
        .json(LANDING_PATH)
    )
    print("Landing schema (response object + discovered path partitions):")
    raw.printSchema()

    return (
        raw
        .filter(F.col("ingest_date") == INGEST_DATE)
        .withColumn("obs", F.explode("observations"))
        .select(
            "series_id",
            F.col("obs.date").alias("date"),
            F.col("obs.value").alias("value"),
        )
        # FRED uses "." for missing observations — drop them.
        .filter(F.col("value") != ".")
    )


def to_bronze(df):
    return (
        df
        .withColumn("value", F.col("value").cast("double"))
        .join(F.broadcast(series_metadata_df()), on="series_id", how="left")
        .withColumn("source", F.lit("fred"))
        .withColumn("api_start_date", F.lit(API_START_DATE))
        .withColumn("api_end_date", F.lit(API_END_DATE))
        .withColumn("ingest_date", F.lit(INGEST_DATE))
        .withColumn("ingested_at", F.current_timestamp())
        .dropDuplicates(["series_id", "date"])
        .select(*BRONZE_COLS)
    )


def main() -> None:
    print("Starting FRED bronze structuring")
    print(f"Ingest date: {INGEST_DATE}")
    print(f"Landing:     {LANDING_PATH}")
    print(f"Bronze:      {BRONZE_PATH}")

    df = read_landing()
    row_count = df.count()
    print(f"Landing observations read: {row_count}")

    if row_count == 0:
        raise ValueError(
            f"No landing data found for fred_macro ingest_date={INGEST_DATE}"
        )

    bronze = to_bronze(df)

    (
        bronze.write
        .mode("overwrite")
        .partitionBy("ingest_date")
        .parquet(BRONZE_PATH)
    )

    print("FRED bronze structuring complete")


main()
job.commit()

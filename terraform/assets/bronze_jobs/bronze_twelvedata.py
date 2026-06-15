import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# Glue Spark job — structures the raw Twelve Data responses from the immutable
# landing zone into columnar Parquet in the bronze layer.
#
# Landing stores the full {meta, values, status} payload verbatim, one file
# per symbol. Here we explode `values` to one row per bar and pull the static
# fields from `meta`. Prices stay as strings (source-shaped); the silver layer
# casts them. Bronze is rebuilt from landing, never the API.

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "landing_bucket",
    "bronze_bucket",
    "ingest_date",
    "interval",
    "api_start_date",
    "api_end_date",
])

LANDING_BUCKET = args["landing_bucket"]
BRONZE_BUCKET  = args["bronze_bucket"]
INGEST_DATE    = args["ingest_date"]
API_START_DATE = args["api_start_date"]
API_END_DATE   = args["api_end_date"]

INTERVAL_MAP = {"1d": "1day", "1w": "1week", "1mo": "1month"}
INTERVAL     = INTERVAL_MAP.get(args["interval"], args["interval"])

LANDING_PATH = f"s3://{LANDING_BUCKET}/equity_prices/"
BRONZE_PATH  = f"s3://{BRONZE_BUCKET}/equity_prices/"

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
# symbol / market / exchange / interval / ingest_date come from the landing
# path partitions; datetime + OHLCV from `values`; the rest from `meta`.
BRONZE_COLS = [
    "datetime", "open", "high", "low", "close", "volume",
    "symbol", "currency", "exchange", "exchange_timezone",
    "mic_code", "instrument_type",
    "source", "market", "interval",
    "api_start_date", "api_end_date", "ingest_date", "ingested_at",
]


def read_landing():
    # Each landing file is one full JSON object {meta, values, status}, so read
    # in multiLine mode. provider/market/exchange/interval/symbol/ingest_date/
    # run_id come from the Hive-partitioned path.
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
        .filter(F.col("interval") == INTERVAL)
        .withColumn("bar", F.explode("values"))
        .select(
            "symbol", "market", "exchange", "interval", "ingest_date",
            F.col("bar.datetime").alias("datetime"),
            F.col("bar.open").alias("open"),
            F.col("bar.high").alias("high"),
            F.col("bar.low").alias("low"),
            F.col("bar.close").alias("close"),
            F.col("bar.volume").alias("volume"),
            F.col("meta.currency").alias("currency"),
            F.col("meta.exchange_timezone").alias("exchange_timezone"),
            F.col("meta.mic_code").alias("mic_code"),
            F.col("meta.type").alias("instrument_type"),
        )
    )


def to_bronze(df):
    return (
        df
        .withColumn("source", F.lit("twelvedata"))
        .withColumn("api_start_date", F.lit(API_START_DATE))
        .withColumn("api_end_date", F.lit(API_END_DATE))
        .withColumn("ingest_date", F.lit(INGEST_DATE))
        .withColumn("ingested_at", F.current_timestamp())
        .dropDuplicates(["symbol", "datetime"])
        .select(*BRONZE_COLS)
    )


def main() -> None:
    print("Starting Twelve Data bronze structuring")
    print(f"Ingest date: {INGEST_DATE}")
    print(f"Interval:    {INTERVAL}")
    print(f"Landing:     {LANDING_PATH}")
    print(f"Bronze:      {BRONZE_PATH}")

    df = read_landing()
    row_count = df.count()
    print(f"Landing rows read: {row_count}")

    if row_count == 0:
        raise ValueError(
            f"No landing data found for equity_prices "
            f"ingest_date={INGEST_DATE} interval={INTERVAL}"
        )

    bronze = to_bronze(df)

    (
        bronze.write
        .mode("overwrite")
        .partitionBy("market", "interval", "ingest_date")
        .parquet(BRONZE_PATH)
    )

    print("Twelve Data bronze structuring complete")


main()
job.commit()

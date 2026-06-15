import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

# Glue Spark job — structures the raw Binance klines pages from the immutable
# landing zone into columnar Parquet in the bronze layer.
#
# Landing stores each page verbatim as a JSON array of arrays. Here we parse
# that with an explicit array<array<string>> schema, explode it to one row per
# kline, and name the 12 positional fields. Values stay source-shaped (prices
# as strings); only the epoch-ms and count fields are cast to long. The silver
# layer does the real typing. Bronze is rebuilt from landing, never the API.

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
INTERVAL       = args["interval"]
API_START_DATE = args["api_start_date"]
API_END_DATE   = args["api_end_date"]

LANDING_PATH = f"s3://{LANDING_BUCKET}/binance_klines/"
BRONZE_PATH  = f"s3://{BRONZE_BUCKET}/binance_klines/"

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Only overwrite the partitions in this run, not the whole table.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
# Keep partition columns as strings instead of letting Spark infer DateType
# from "ingest_date=2024-01-01" dir names — matches how we filter and write.
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

# Binance kline field order, per API docs. The raw page is a JSON array of
# arrays, so each kline arrives positionally.
KLINE_FIELDS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trade_count",
    "taker_buy_base_volume", "taker_buy_quote_volume", "ignore",
]
PAGE_SCHEMA = ArrayType(ArrayType(StringType()))
LONG_COLS = ["open_time", "close_time", "trade_count"]

# Final bronze column order — mirrors the original source-shaped schema.
# symbol / interval arrive from the landing path partitions (see read_landing).
BRONZE_COLS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trade_count",
    "taker_buy_base_volume", "taker_buy_quote_volume", "ignore",
    "source", "symbol", "interval",
    "api_start_date", "api_end_date", "ingest_date", "ingested_at",
]


def read_landing():
    # Each landing file is a whole raw page (array of arrays), so read entire
    # files (wholetext) rather than line-by-line. symbol/interval/ingest_date/
    # run_id come from the Hive-partitioned path.
    raw = (
        spark.read
        .option("wholetext", "true")
        .text(LANDING_PATH)
    )
    print("Landing schema (file text + discovered path partitions):")
    raw.printSchema()

    klines = (
        raw
        .filter(F.col("ingest_date") == INGEST_DATE)
        .filter(F.col("interval") == INTERVAL)
        .withColumn("kline", F.explode(F.from_json("value", PAGE_SCHEMA)))
    )

    cols = [
        F.col("kline")[i].alias(name)
        for i, name in enumerate(KLINE_FIELDS)
    ]
    return klines.select("symbol", "interval", "ingest_date", *cols)


def to_bronze(df):
    for c in LONG_COLS:
        df = df.withColumn(c, F.col(c).cast("long"))

    return (
        df
        .withColumn("source", F.lit("binance"))
        .withColumn("api_start_date", F.lit(API_START_DATE))
        .withColumn("api_end_date", F.lit(API_END_DATE))
        # Re-stamp as a string literal so the partition column type is stable.
        .withColumn("ingest_date", F.lit(INGEST_DATE))
        .withColumn("ingested_at", F.current_timestamp())
        # Multiple landing run_ids for one date collapse to one row per key.
        .dropDuplicates(["symbol", "open_time"])
        .select(*BRONZE_COLS)
    )


def main() -> None:
    print("Starting Binance bronze structuring")
    print(f"Ingest date: {INGEST_DATE}")
    print(f"Interval:    {INTERVAL}")
    print(f"Landing:     {LANDING_PATH}")
    print(f"Bronze:      {BRONZE_PATH}")

    df = read_landing()
    row_count = df.count()
    print(f"Landing klines read: {row_count}")

    if row_count == 0:
        raise ValueError(
            f"No landing data found for binance_klines "
            f"ingest_date={INGEST_DATE} interval={INTERVAL}"
        )

    bronze = to_bronze(df)

    (
        bronze.write
        .mode("overwrite")
        .partitionBy("symbol", "interval", "ingest_date")
        .parquet(BRONZE_PATH)
    )

    print("Binance bronze structuring complete")


main()
job.commit()

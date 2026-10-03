import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "bronze_bucket",
    "silver_bucket",
    "catalog_database",
    "table_name",
    "ingest_date",
    "interval",
])

BRONZE_BUCKET    = args["bronze_bucket"]
SILVER_BUCKET    = args["silver_bucket"]
CATALOG_DATABASE = args["catalog_database"]
TABLE_NAME       = args["table_name"]
INGEST_DATE      = args["ingest_date"]
INTERVAL_MAP     = {"1d": "1day", "1w": "1week", "1mo": "1month"}
INTERVAL         = INTERVAL_MAP.get(args["interval"], args["interval"])

FULL_TABLE_NAME = f"glue_catalog.{CATALOG_DATABASE}.{TABLE_NAME}"
BRONZE_PATH     = f"s3://{BRONZE_BUCKET}/equity_prices/"

conf = SparkConf()
conf.set(
    "spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
)
conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{SILVER_BUCKET}/")
conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
# Read bronze partition columns (market, interval, ingest_date) as strings
# rather than inferring DateType from "ingest_date=2024-01-01" dir names.
conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

sc          = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

NUMERIC_COLS = [
    "open", "high", "low", "close", "volume",
]

DROP_COLS = ["api_start_date", "api_end_date", "ingested_at"]


def read_bronze():
    return (
        # mergeSchema: older bronze batches predate the lineage columns.
        spark.read.option("mergeSchema", "true").parquet(BRONZE_PATH)
        .filter(
            (F.col("ingest_date") == INGEST_DATE) &
            (F.col("interval") == INTERVAL)
        )
    )


def transform(df):
    df = (
        df
        # Twelve Data returns datetime as date-only ("2024-01-02") for daily
        # intervals and with a time component for intraday — try both so daily
        # data doesn't parse to NULL.
        .withColumn(
            "datetime",
            F.coalesce(
                F.to_timestamp(F.col("datetime"), "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp(F.col("datetime"), "yyyy-MM-dd"),
            ),
        )
        .withColumn("date", F.to_date(F.col("datetime")))
    )

    for c in NUMERIC_COLS:
        df = df.withColumn(c, F.col(c).cast(DoubleType()))

    df = (
        df
        .drop(*DROP_COLS)
        # Deduplicate before merge — duplicate rows on the natural key cause MERGE to fail
        .dropDuplicates(["symbol", "datetime"])
        # transformed_at = last write; first_loaded_at keeps the original
        # load time across MERGE updates (see write_silver).
        .withColumn("transformed_at", F.current_timestamp())
        .withColumn("first_loaded_at", F.current_timestamp())
    )

    return df


def table_exists() -> bool:
    return TABLE_NAME in [t.name for t in spark.catalog.listTables(CATALOG_DATABASE)]


def add_missing_columns(df) -> None:
    """Schema evolution: add columns the table doesn't have yet (e.g. lineage)."""
    existing = set(spark.table(FULL_TABLE_NAME).columns)
    for field in df.schema.fields:
        if field.name not in existing:
            print(f"Adding column {field.name} {field.dataType.simpleString()}")
            spark.sql(
                f"ALTER TABLE {FULL_TABLE_NAME} "
                f"ADD COLUMNS (`{field.name}` {field.dataType.simpleString()})"
            )


def write_silver(df) -> None:
    if not table_exists():
        print(f"{FULL_TABLE_NAME} does not exist — creating")
        (
            df.writeTo(FULL_TABLE_NAME)
            .tableProperty("format-version", "2")
            # Rows aren't pre-sorted by year; fanout keeps one open file per
            # partition instead of failing. Persisted, so MERGE uses it too.
            .tableProperty("write.spark.fanout.enabled", "true")
            # Yearly partitions: daily data is tiny, so partitioning by date meant one
            # file per row and multi-minute MERGEs. A year holds ~2k rows.
            .partitionedBy(F.years("date"))
            .createOrReplace()
        )
    else:
        print(f"{FULL_TABLE_NAME} exists — merging")
        add_missing_columns(df)
        updates = ", ".join(
            f"t.`{c}` = s.`{c}`" for c in df.columns if c != "first_loaded_at"
        )
        df.createOrReplaceTempView("new_data")
        spark.sql(f"""
            MERGE INTO {FULL_TABLE_NAME} t
            USING new_data s
            ON t.date = s.date AND t.symbol = s.symbol AND t.datetime = s.datetime
            WHEN MATCHED THEN UPDATE SET {updates},
                t.first_loaded_at = coalesce(t.first_loaded_at, s.first_loaded_at)
            WHEN NOT MATCHED THEN INSERT *
        """)


def main() -> None:
    print(f"Starting equity prices silver transform")
    print(f"Ingest date: {INGEST_DATE}")
    print(f"Interval:    {INTERVAL}")
    print(f"Table:       {FULL_TABLE_NAME}")

    df = read_bronze()
    row_count = df.count()
    print(f"Bronze rows read: {row_count}")

    if row_count == 0:
        raise ValueError(
            f"No bronze data found for equity_prices "
            f"ingest_date={INGEST_DATE} interval={INTERVAL}"
        )

    df = transform(df)

    write_silver(df)

    print("Equity prices silver transform complete")


main()
job.commit()
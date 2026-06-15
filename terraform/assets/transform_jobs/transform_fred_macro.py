import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "bronze_bucket",
    "silver_bucket",
    "catalog_database",
    "table_name",
    "ingest_date",
])

BRONZE_BUCKET    = args["bronze_bucket"]
SILVER_BUCKET    = args["silver_bucket"]
CATALOG_DATABASE = args["catalog_database"]
TABLE_NAME       = args["table_name"]
INGEST_DATE      = args["ingest_date"]

FULL_TABLE_NAME = f"glue_catalog.{CATALOG_DATABASE}.{TABLE_NAME}"
BRONZE_PATH     = f"s3://{BRONZE_BUCKET}/fred_macro/"

conf = SparkConf()
conf.set(
    "spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
)
conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{SILVER_BUCKET}/")
conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
# Read the bronze partition column (ingest_date) as a string rather than
# inferring DateType from "ingest_date=2024-01-01" dir names.
conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

sc          = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

DROP_COLS = ["api_start_date", "api_end_date", "ingested_at"]


def read_bronze():
    return (
        spark.read.parquet(BRONZE_PATH)
        .filter(F.col("ingest_date") == INGEST_DATE)
    )


def transform(df):
    df = (
        df
        .withColumn("date", F.to_date(F.col("date")))
        .drop(*DROP_COLS)
        # Deduplicate before merge — duplicate rows on the natural key cause MERGE to fail
        .dropDuplicates(["series_id", "date"])
        .withColumn("transformed_at", F.current_timestamp())
    )

    return df


def table_exists() -> bool:
    return TABLE_NAME in [t.name for t in spark.catalog.listTables(CATALOG_DATABASE)]


def write_silver(df) -> None:
    if not table_exists():
        print(f"{FULL_TABLE_NAME} does not exist — creating")
        (
            df.writeTo(FULL_TABLE_NAME)
            .tableProperty("format-version", "2")
            .partitionedBy("series_id", "date")
            .createOrReplace()
        )
    else:
        print(f"{FULL_TABLE_NAME} exists — merging")
        df.createOrReplaceTempView("new_data")
        spark.sql(f"""
            MERGE INTO {FULL_TABLE_NAME} t
            USING new_data s
            ON t.series_id = s.series_id AND t.date = s.date
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)


def main() -> None:
    print(f"Starting FRED macro silver transform")
    print(f"Ingest date: {INGEST_DATE}")
    print(f"Table:       {FULL_TABLE_NAME}")

    df = read_bronze()
    row_count = df.count()
    print(f"Bronze rows read: {row_count}")

    if row_count == 0:
        raise ValueError(
            f"No bronze data found for fred_macro "
            f"ingest_date={INGEST_DATE}"
        )

    df = transform(df)

    write_silver(df)

    print("FRED macro silver transform complete")


main()
job.commit()
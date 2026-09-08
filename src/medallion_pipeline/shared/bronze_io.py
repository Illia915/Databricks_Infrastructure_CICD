"""Bronze ingestion: preserve source values and add ingestion metadata."""

from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from medallion_pipeline.shared.schemas import (
    CUSTOMERS_RAW_SCHEMA,
    PRODUCTS_RAW_SCHEMA,
    SALES_RAW_SCHEMA,
)


def read_csv_with_metadata(
    spark: SparkSession,
    path: str,
    schema: StructType,
    batch_id: str,
) -> DataFrame:
    return (
        spark.read.option("header", True)
        .schema(schema)
        .csv(path)
        .withColumn("_source_file", F.input_file_name())
        .withColumn(
            "_ingested_at",
            F.lit(datetime.now(UTC).replace(tzinfo=None)).cast("timestamp"),
        )
        .withColumn("_batch_id", F.lit(batch_id))
    )


def ingest_sources(
    spark: SparkSession,
    source_path: str,
    batch_id: str,
) -> dict[str, DataFrame]:
    return {
        "customers": read_csv_with_metadata(
            spark, f"{source_path}/customers.csv", CUSTOMERS_RAW_SCHEMA, batch_id
        ),
        "products": read_csv_with_metadata(
            spark, f"{source_path}/products.csv", PRODUCTS_RAW_SCHEMA, batch_id
        ),
        "sales": read_csv_with_metadata(
            spark, f"{source_path}/sales.csv", SALES_RAW_SCHEMA, batch_id
        ),
    }

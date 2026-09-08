"""Databricks task: ingest products.csv into Bronze."""

from medallion_pipeline.shared.bronze_io import read_csv_with_metadata
from medallion_pipeline.shared.runtime import ensure_schemas, task_config, write_delta
from medallion_pipeline.shared.schemas import PRODUCTS_RAW_SCHEMA


def main() -> None:
    spark, config = task_config()
    ensure_schemas(spark, config)
    products = read_csv_with_metadata(
        spark,
        f"{config.source_path}/products.csv",
        PRODUCTS_RAW_SCHEMA,
        config.batch_id,
    )
    write_delta(products, config.table_name(config.bronze_schema, "products_raw"))

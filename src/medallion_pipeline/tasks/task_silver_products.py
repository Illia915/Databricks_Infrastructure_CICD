"""Databricks task: clean Bronze products and write Silver/quarantine."""

from medallion_pipeline.shared.runtime import ensure_schemas, task_config, write_delta
from medallion_pipeline.shared.silver import clean_products


def main() -> None:
    spark, config = task_config()
    ensure_schemas(spark, config)
    source = spark.table(config.table_name(config.bronze_schema, "products_raw"))
    products, rejected = clean_products(source)
    write_delta(products, config.table_name(config.silver_schema, "products"))
    write_delta(
        rejected,
        config.table_name(config.quarantine_schema, "products_rejected"),
    )

"""Databricks task: clean Bronze sales and write Silver/quarantine."""

from medallion_pipeline.shared.runtime import ensure_schemas, task_config, write_delta
from medallion_pipeline.shared.silver import clean_sales


def main() -> None:
    spark, config = task_config()
    ensure_schemas(spark, config)
    sales = spark.table(config.table_name(config.bronze_schema, "sales_raw"))
    customers = spark.table(config.table_name(config.silver_schema, "customers"))
    products = spark.table(config.table_name(config.silver_schema, "products"))
    valid_sales, rejected = clean_sales(sales, customers, products)
    write_delta(valid_sales, config.table_name(config.silver_schema, "sales"))
    write_delta(rejected, config.table_name(config.quarantine_schema, "sales_rejected"))


if __name__ == "__main__":
    main()

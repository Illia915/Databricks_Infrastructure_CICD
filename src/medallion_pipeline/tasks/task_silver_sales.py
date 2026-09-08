"""Databricks task: clean Bronze sales and write Silver/quarantine."""

import sys
from pathlib import Path

for parent in (Path.cwd(), *Path.cwd().parents):
    source_root = parent / "src"
    if (source_root / "medallion_pipeline").is_dir():
        sys.path.insert(0, str(source_root))
        break


def main() -> None:
    from medallion_pipeline.shared.runtime import ensure_schemas, task_config, write_delta
    from medallion_pipeline.shared.silver import clean_sales

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

"""Databricks task: build the Gold daily sales summary."""

import sys
from pathlib import Path

for parent in (Path.cwd(), *Path.cwd().parents):
    source_root = parent / "src"
    if (source_root / "medallion_pipeline").is_dir():
        sys.path.insert(0, str(source_root))
        break


def main() -> None:
    from medallion_pipeline.shared.gold import build_daily_sales_summary
    from medallion_pipeline.shared.runtime import ensure_schemas, task_config, write_delta

    spark, config = task_config()
    ensure_schemas(spark, config)
    fact_sales = spark.table(config.table_name(config.gold_schema, "fact_sales"))
    products = spark.table(config.table_name(config.silver_schema, "products"))
    write_delta(
        build_daily_sales_summary(fact_sales, products),
        config.table_name(config.gold_schema, "daily_sales_summary"),
    )


if __name__ == "__main__":
    main()

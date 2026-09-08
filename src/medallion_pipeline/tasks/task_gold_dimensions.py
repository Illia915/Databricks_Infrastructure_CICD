"""Databricks task: build Gold customer and product dimensions."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def main() -> None:
    from medallion_pipeline.shared.gold import build_dim_customer, build_dim_product
    from medallion_pipeline.shared.runtime import ensure_schemas, task_config, write_delta

    spark, config = task_config()
    ensure_schemas(spark, config)
    customers = spark.table(config.table_name(config.silver_schema, "customers"))
    products = spark.table(config.table_name(config.silver_schema, "products"))
    write_delta(
        build_dim_customer(customers),
        config.table_name(config.gold_schema, "dim_customer"),
    )
    write_delta(
        build_dim_product(products),
        config.table_name(config.gold_schema, "dim_product"),
    )


if __name__ == "__main__":
    main()

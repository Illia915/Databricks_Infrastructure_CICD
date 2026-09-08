"""Databricks task: build the Gold sales fact."""

from medallion_pipeline.shared.gold import build_fact_sales
from medallion_pipeline.shared.runtime import ensure_schemas, task_config, write_delta


def main() -> None:
    spark, config = task_config()
    ensure_schemas(spark, config)
    sales = spark.table(config.table_name(config.silver_schema, "sales"))
    write_delta(
        build_fact_sales(sales),
        config.table_name(config.gold_schema, "fact_sales"),
    )


if __name__ == "__main__":
    main()

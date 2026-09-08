"""Databricks task: clean Bronze customers and write Silver/quarantine."""

import sys
from pathlib import Path

for parent in (Path.cwd(), *Path.cwd().parents):
    source_root = parent / "src"
    if (source_root / "medallion_pipeline").is_dir():
        sys.path.insert(0, str(source_root))
        break


def main() -> None:
    from medallion_pipeline.shared.runtime import ensure_schemas, task_config, write_delta
    from medallion_pipeline.shared.silver import clean_customers

    spark, config = task_config()
    ensure_schemas(spark, config)
    source = spark.table(config.table_name(config.bronze_schema, "customers_raw"))
    customers, rejected = clean_customers(source)
    write_delta(customers, config.table_name(config.silver_schema, "customers"))
    write_delta(
        rejected,
        config.table_name(config.quarantine_schema, "customers_rejected"),
    )


if __name__ == "__main__":
    main()

"""Databricks task: ingest customers.csv into Bronze."""

import sys
from pathlib import Path

for parent in (Path.cwd(), *Path.cwd().parents):
    source_root = parent / "src"
    if (source_root / "medallion_pipeline").is_dir():
        sys.path.insert(0, str(source_root))
        break


def main() -> None:
    from medallion_pipeline.shared.bronze_io import read_csv_with_metadata
    from medallion_pipeline.shared.runtime import ensure_schemas, task_config, write_delta
    from medallion_pipeline.shared.schemas import CUSTOMERS_RAW_SCHEMA

    spark, config = task_config()
    ensure_schemas(spark, config)
    customers = read_csv_with_metadata(
        spark,
        f"{config.source_path}/customers.csv",
        CUSTOMERS_RAW_SCHEMA,
        config.batch_id,
    )
    write_delta(customers, config.table_name(config.bronze_schema, "customers_raw"))


if __name__ == "__main__":
    main()

"""Databricks task: ingest sales.csv into Bronze."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def main() -> None:
    from medallion_pipeline.shared.bronze_io import read_csv_with_metadata
    from medallion_pipeline.shared.runtime import ensure_schemas, task_config, write_delta
    from medallion_pipeline.shared.schemas import SALES_RAW_SCHEMA

    spark, config = task_config()
    ensure_schemas(spark, config)
    sales = read_csv_with_metadata(
        spark,
        f"{config.source_path}/sales.csv",
        SALES_RAW_SCHEMA,
        config.batch_id,
    )
    write_delta(sales, config.table_name(config.bronze_schema, "sales_raw"))


if __name__ == "__main__":
    main()

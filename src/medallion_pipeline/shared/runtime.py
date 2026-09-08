"""Widget-backed runtime configuration for Databricks notebooks."""

from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame, SparkSession

DEFAULT_SOURCE_PATH = "/Volumes/codex_cicd_dev/source/source_data"


@dataclass(frozen=True)
class PipelineConfig:
    catalog_name: str
    source_path: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    quarantine_schema: str
    batch_id: str

    def table_name(self, schema: str, table: str) -> str:
        return f"{self.catalog_name}.{schema}.{table}"


def _widget_value(dbutils: Any, name: str, default: str) -> str:
    widgets = dbutils.widgets
    with suppress(Exception):
        widgets.text(name, default)
    return str(widgets.get(name))


def config_from_widgets(dbutils: Any) -> PipelineConfig:
    """Read all environment-dependent values from Databricks widgets."""

    return PipelineConfig(
        catalog_name=_widget_value(dbutils, "catalog_name", "codex_cicd_dev"),
        source_path=_widget_value(dbutils, "source_path", DEFAULT_SOURCE_PATH),
        bronze_schema=_widget_value(dbutils, "bronze_schema", "bronze"),
        silver_schema=_widget_value(dbutils, "silver_schema", "silver"),
        gold_schema=_widget_value(dbutils, "gold_schema", "gold"),
        quarantine_schema=_widget_value(dbutils, "quarantine_schema", "quarantine"),
        batch_id=_widget_value(dbutils, "batch_id", "manual"),
    )


def active_spark() -> SparkSession:
    """Return the Databricks-provided active Spark session without creating one."""

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active Spark session. Run this task on Databricks.")
    return spark


def databricks_dbutils(spark: SparkSession) -> Any:
    """Create the Databricks utility wrapper around the active session."""

    from pyspark.dbutils import DBUtils

    return DBUtils(spark)


def task_config() -> tuple[SparkSession, PipelineConfig]:
    spark = active_spark()
    return spark, config_from_widgets(databricks_dbutils(spark))


def ensure_schemas(spark: SparkSession, config: PipelineConfig) -> None:
    for schema in (
        config.bronze_schema,
        config.silver_schema,
        config.gold_schema,
        config.quarantine_schema,
    ):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{config.catalog_name}`.`{schema}`")


def write_delta(df: DataFrame, table_name: str) -> None:
    df.write.format("delta").mode("append").option("mergeSchema", "false").saveAsTable(table_name)

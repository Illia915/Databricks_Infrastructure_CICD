"""Contract tests for three wide pipeline outputs.

This pattern is intended for tables with 30+ attributes. Keep the contract
declarative and validate the complete schema, then add focused business-rule
tests for calculations and relationships.
"""

from collections.abc import Iterable

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

CUSTOMER_OUTPUT_SCHEMA = StructType(
    [
        StructField("customer_id", IntegerType(), False),
        StructField("customer_number", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("address_line_1", StringType(), True),
        StructField("address_line_2", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state_province", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("lifetime_order_count", IntegerType(), False),
        StructField("lifetime_units", IntegerType(), False),
        StructField("lifetime_revenue", DecimalType(18, 2), False),
        StructField("first_order_date", DateType(), True),
        StructField("last_order_date", DateType(), True),
        StructField("is_active", BooleanType(), False),
        StructField("marketing_opt_in", BooleanType(), False),
        StructField("source_system", StringType(), False),
        StructField("record_created_at", StringType(), False),
        StructField("record_updated_at", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("record_hash", StringType(), False),
        StructField("data_quality_status", StringType(), False),
        StructField("effective_from", DateType(), False),
        StructField("effective_to", DateType(), True),
        StructField("is_current", BooleanType(), False),
        StructField("pipeline_run_id", StringType(), False),
    ]
)

PRODUCT_OUTPUT_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), False),
        StructField("sku", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("description", StringType(), True),
        StructField("category", StringType(), False),
        StructField("subcategory", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("supplier_id", StringType(), False),
        StructField("supplier_name", StringType(), True),
        StructField("unit_price", DecimalType(18, 2), False),
        StructField("cost_price", DecimalType(18, 2), True),
        StructField("currency_code", StringType(), False),
        StructField("tax_rate", DecimalType(5, 2), False),
        StructField("weight_kg", DecimalType(10, 3), True),
        StructField("is_active", BooleanType(), False),
        StructField("is_discontinued", BooleanType(), False),
        StructField("inventory_quantity", IntegerType(), False),
        StructField("reorder_level", IntegerType(), False),
        StructField("first_available_date", DateType(), True),
        StructField("last_restock_date", DateType(), True),
        StructField("source_system", StringType(), False),
        StructField("record_created_at", StringType(), False),
        StructField("record_updated_at", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("record_hash", StringType(), False),
        StructField("data_quality_status", StringType(), False),
        StructField("effective_from", DateType(), False),
        StructField("effective_to", DateType(), True),
        StructField("is_current", BooleanType(), False),
        StructField("pipeline_run_id", StringType(), False),
    ]
)

SALES_OUTPUT_SCHEMA = StructType(
    [
        StructField("sale_id", IntegerType(), False),
        StructField("order_number", StringType(), False),
        StructField("order_date", DateType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("product_id", IntegerType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DecimalType(18, 2), False),
        StructField("gross_amount", DecimalType(18, 2), False),
        StructField("discount_pct", DecimalType(5, 2), False),
        StructField("discount_amount", DecimalType(18, 2), False),
        StructField("tax_amount", DecimalType(18, 2), False),
        StructField("shipping_amount", DecimalType(18, 2), False),
        StructField("net_amount", DecimalType(18, 2), False),
        StructField("currency_code", StringType(), False),
        StructField("sales_channel", StringType(), False),
        StructField("payment_method", StringType(), False),
        StructField("order_status", StringType(), False),
        StructField("is_refunded", BooleanType(), False),
        StructField("refund_amount", DecimalType(18, 2), False),
        StructField("customer_segment", StringType(), True),
        StructField("product_category", StringType(), False),
        StructField("supplier_id", StringType(), False),
        StructField("order_year", IntegerType(), False),
        StructField("order_month", IntegerType(), False),
        StructField("order_week", IntegerType(), False),
        StructField("source_system", StringType(), False),
        StructField("record_created_at", StringType(), False),
        StructField("record_updated_at", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("record_hash", StringType(), False),
        StructField("data_quality_status", StringType(), False),
        StructField("pipeline_run_id", StringType(), False),
    ]
)

OUTPUT_CONTRACTS = {
    "dim_customer": CUSTOMER_OUTPUT_SCHEMA,
    "dim_product": PRODUCT_OUTPUT_SCHEMA,
    "fact_sales": SALES_OUTPUT_SCHEMA,
}


def assert_schema_matches(actual: StructType, expected: StructType) -> None:
    """Compare all columns and metadata in one readable assertion."""

    assert actual == expected, (
        "Schema contract mismatch.\n"
        f"Expected: {expected.simpleString()}\n"
        f"Actual:   {actual.simpleString()}"
    )


def assert_required_columns_present(df: DataFrame, required: Iterable[str]) -> None:
    missing = sorted(set(required) - set(df.columns))
    assert not missing, f"Missing required columns: {missing}"


def assert_required_fields_present(schema: StructType, required: Iterable[str]) -> None:
    missing = sorted(set(required) - set(schema.fieldNames()))
    assert not missing, f"Missing required fields: {missing}"


def assert_unique_key(df: DataFrame, key_columns: list[str]) -> None:
    duplicate_keys = df.groupBy(*key_columns).count().filter("count > 1").limit(10).collect()
    assert not duplicate_keys, f"Duplicate keys found for {key_columns}: {duplicate_keys}"


@pytest.mark.contract
@pytest.mark.parametrize(
    ("table_name", "schema"),
    list(OUTPUT_CONTRACTS.items()),
)
def test_output_table_schema_contract(
    table_name: str,
    schema: StructType,
) -> None:
    assert len(schema.fields) >= 30, f"{table_name} should be a wide 30+ column table"
    assert len(schema.fieldNames()) == len(set(schema.fieldNames()))


@pytest.mark.contract
def test_output_contracts_have_required_audit_columns() -> None:
    required_audit_columns = {
        "source_system",
        "record_created_at",
        "record_updated_at",
        "batch_id",
        "record_hash",
        "data_quality_status",
        "pipeline_run_id",
    }

    for _table_name, schema in OUTPUT_CONTRACTS.items():
        assert_required_fields_present(schema, required_audit_columns)


@pytest.mark.contract
def test_output_contracts_have_non_nullable_business_keys() -> None:
    expected_keys = {
        "dim_customer": "customer_id",
        "dim_product": "product_id",
        "fact_sales": "sale_id",
    }

    for table_name, key in expected_keys.items():
        field = OUTPUT_CONTRACTS[table_name][key]
        assert field.nullable is False, f"{table_name}.{key} must be non-nullable"


@pytest.mark.contract
def test_output_dataframe_can_be_validated_against_contracts(
    spark: SparkSession,
) -> None:
    """Example runtime contract check against three DataFrames."""

    output_dataframes = {
        name: spark.createDataFrame([], schema) for name, schema in OUTPUT_CONTRACTS.items()
    }

    for table_name, df in output_dataframes.items():
        assert_schema_matches(df.schema, OUTPUT_CONTRACTS[table_name])
        assert_required_columns_present(df, OUTPUT_CONTRACTS[table_name].fieldNames())

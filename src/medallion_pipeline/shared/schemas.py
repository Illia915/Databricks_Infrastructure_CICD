"""Explicit schemas for the source and medallion tables."""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CUSTOMERS_RAW_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField("customer_segment", StringType(), True),
    ]
)

PRODUCTS_RAW_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("unit_price", StringType(), True),
        StructField("supplier_id", StringType(), True),
        StructField("is_active", StringType(), True),
    ]
)

SALES_RAW_SCHEMA = StructType(
    [
        StructField("sale_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("unit_price", StringType(), True),
        StructField("discount_pct", StringType(), True),
        StructField("total_amount", StringType(), True),
        StructField("sales_channel", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("order_status", StringType(), True),
    ]
)

CUSTOMER_SILVER_SCHEMA = StructType(
    [
        StructField("customer_id", IntegerType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("signup_date", DateType(), True),
        StructField("customer_segment", StringType(), True),
    ]
)

PRODUCT_SILVER_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("unit_price", DecimalType(12, 2), True),
        StructField("supplier_id", StringType(), True),
        StructField("is_active", BooleanType(), True),
    ]
)

SALES_SILVER_SCHEMA = StructType(
    [
        StructField("sale_id", IntegerType(), False),
        StructField("order_date", DateType(), True),
        StructField("customer_id", IntegerType(), False),
        StructField("product_id", IntegerType(), False),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DecimalType(12, 2), True),
        StructField("discount_pct", DecimalType(5, 2), True),
        StructField("total_amount", DecimalType(14, 2), True),
        StructField("sales_channel", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("order_status", StringType(), True),
    ]
)

INGESTION_FIELDS = (
    StructField("_source_file", StringType(), False),
    StructField("_ingested_at", TimestampType(), False),
    StructField("_batch_id", StringType(), False),
)

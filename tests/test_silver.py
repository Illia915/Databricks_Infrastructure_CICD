from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from medallion_pipeline.shared.silver import clean_customers, clean_sales


@pytest.mark.unit
def test_clean_customers_normalizes_email_and_date(spark: SparkSession) -> None:
    source = spark.createDataFrame(
        [
            Row(
                customer_id="1",
                first_name=" Ada ",
                last_name="Lovelace",
                email=" ADA@EXAMPLE.COM ",
                city=" London ",
                country="UK",
                signup_date="2024-01-02",
                customer_segment="consumer",
                _source_file="customers.csv",
                _ingested_at=None,
                _batch_id="test",
            )
        ],
        schema=StructType(
            [
                StructField("customer_id", StringType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("city", StringType(), True),
                StructField("country", StringType(), True),
                StructField("signup_date", StringType(), True),
                StructField("customer_segment", StringType(), True),
                StructField("_source_file", StringType(), True),
                StructField("_ingested_at", TimestampType(), True),
                StructField("_batch_id", StringType(), True),
            ]
        ),
    )

    accepted, rejected = clean_customers(source)

    assert accepted.select("customer_id", "email", "signup_date").collect() == [
        Row(customer_id=1, email="ada@example.com", signup_date=date(2024, 1, 2))
    ]
    assert rejected.count() == 0


@pytest.mark.unit
def test_clean_sales_quarantines_broken_reference_and_invalid_discount(
    spark: SparkSession,
) -> None:
    customers = spark.createDataFrame(
        [
            Row(
                customer_id=1,
                signup_date=date(2024, 1, 1),
                _ingested_at=None,
            )
        ],
        schema=StructType(
            [
                StructField("customer_id", IntegerType(), False),
                StructField("signup_date", DateType(), True),
                StructField("_ingested_at", TimestampType(), True),
            ]
        ),
    )
    products = spark.createDataFrame(
        [Row(product_id=10, unit_price=Decimal("20.00"), _ingested_at=None)],
        schema=StructType(
            [
                StructField("product_id", IntegerType(), False),
                StructField("unit_price", DecimalType(12, 2), False),
                StructField("_ingested_at", TimestampType(), True),
            ]
        ),
    )
    source = spark.createDataFrame(
        [
            Row(
                sale_id="1",
                order_date="2024-02-01",
                customer_id="1",
                product_id="10",
                quantity="2",
                unit_price="20.00",
                discount_pct="10.00",
                total_amount="36.00",
                sales_channel="web",
                payment_method="card",
                order_status="completed",
                _source_file="sales.csv",
                _ingested_at=None,
                _batch_id="test",
            ),
            Row(
                sale_id="2",
                order_date="2024-02-01",
                customer_id="1",
                product_id="10",
                quantity="2",
                unit_price="20.00",
                discount_pct="110.00",
                total_amount="-4.00",
                sales_channel="web",
                payment_method="card",
                order_status="completed",
                _source_file="sales.csv",
                _ingested_at=None,
                _batch_id="test",
            ),
        ],
        schema=StructType(
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
                StructField("_source_file", StringType(), True),
                StructField("_ingested_at", TimestampType(), True),
                StructField("_batch_id", StringType(), True),
            ]
        ),
    )

    accepted, rejected = clean_sales(source, customers, products)

    assert [row.sale_id for row in accepted.select("sale_id").collect()] == [1]
    assert rejected.select("sale_id").collect() == [Row(sale_id=2)]

from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import Row, SparkSession

from medallion_pipeline.shared.gold import (
    build_daily_sales_summary,
    build_fact_sales,
)


@pytest.mark.unit
def test_fact_sales_calculates_gross_and_discount(spark: SparkSession) -> None:
    sales = spark.createDataFrame(
        [
            Row(
                sale_id=1,
                order_date=date(2024, 1, 1),
                customer_id=1,
                product_id=10,
                quantity=2,
                unit_price=Decimal("20.00"),
                discount_pct=Decimal("10.00"),
                total_amount=Decimal("36.00"),
                sales_channel="web",
                payment_method="card",
                order_status="completed",
            )
        ]
    )

    result = (
        build_fact_sales(sales).select("gross_amount", "discount_amount", "net_amount").collect()
    )

    assert result == [
        Row(
            gross_amount=Decimal("40.00"),
            discount_amount=Decimal("4.00"),
            net_amount=Decimal("36.00"),
        )
    ]


@pytest.mark.contract
def test_daily_summary_has_expected_grain_and_metrics(spark: SparkSession) -> None:
    fact = spark.createDataFrame(
        [
            Row(
                order_date=date(2024, 1, 1),
                customer_id=1,
                product_id=10,
                quantity=2,
                net_amount=Decimal("36.00"),
                gross_amount=Decimal("40.00"),
                discount_amount=Decimal("4.00"),
                sales_channel="web",
                order_status="completed",
            )
        ]
    )
    products = spark.createDataFrame([Row(product_id=10, category="Books")])

    result = build_daily_sales_summary(fact, products).collect()

    assert len(result) == 1
    assert result[0].order_count == 1
    assert result[0].units_sold == 2
    assert result[0].net_revenue == Decimal("36.00")

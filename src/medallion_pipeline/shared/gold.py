"""Gold facts, dimensions, and business aggregates."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_dim_customer(customers: DataFrame) -> DataFrame:
    return customers.select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "city",
        "country",
        "signup_date",
        "customer_segment",
    )


def build_dim_product(products: DataFrame) -> DataFrame:
    return products.select(
        "product_id",
        "product_name",
        "category",
        "unit_price",
        "supplier_id",
        "is_active",
    )


def build_fact_sales(sales: DataFrame) -> DataFrame:
    return (
        sales.withColumn(
            "gross_amount",
            (F.col("unit_price") * F.col("quantity")).cast("decimal(14,2)"),
        )
        .withColumn(
            "discount_amount",
            (F.col("gross_amount") * F.col("discount_pct") / F.lit(100)).cast("decimal(14,2)"),
        )
        .withColumnRenamed("total_amount", "net_amount")
    )


def build_daily_sales_summary(fact_sales: DataFrame, products: DataFrame) -> DataFrame:
    return (
        fact_sales.join(products.select("product_id", "category"), "product_id")
        .groupBy("order_date", "category", "sales_channel", "order_status")
        .agg(
            F.count("*").alias("order_count"),
            F.countDistinct("customer_id").alias("distinct_customer_count"),
            F.sum("quantity").alias("units_sold"),
            F.sum("gross_amount").cast("decimal(18,2)").alias("gross_revenue"),
            F.sum("discount_amount").cast("decimal(18,2)").alias("discount_amount"),
            F.sum("net_amount").cast("decimal(18,2)").alias("net_revenue"),
            F.avg("net_amount").cast("decimal(18,2)").alias("average_order_value"),
        )
    )

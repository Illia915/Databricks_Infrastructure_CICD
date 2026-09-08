"""Silver cleansing, validation, and quarantine transformations."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

VALID_SEGMENTS = ("consumer", "small_business", "enterprise")
VALID_CHANNELS = ("web", "mobile", "store", "partner")
VALID_PAYMENTS = ("card", "bank_transfer", "digital_wallet", "cash")
VALID_STATUSES = ("completed", "shipped", "refunded")


def _clean_strings(df: DataFrame) -> DataFrame:
    return df.select(
        *[
            F.when(F.trim(F.col(column)) == "", None).otherwise(F.trim(F.col(column))).alias(column)
            for column in df.columns
        ]
    )


def clean_customers(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    cleaned = (
        _clean_strings(df)
        .withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("signup_date", F.to_date("signup_date"))
        .withColumn("email", F.lower("email"))
    )
    invalid = (
        F.col("customer_id").isNull()
        | F.col("email").isNull()
        | F.col("signup_date").isNull()
        | ~F.col("customer_segment").isin(*VALID_SEGMENTS)
    )
    return _split_valid_rejected(cleaned, invalid, "invalid_customer_record", "customer_id")


def clean_products(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    cleaned = (
        _clean_strings(df)
        .withColumn("product_id", F.col("product_id").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(12,2)"))
        .withColumn("is_active", F.lower("is_active").cast("boolean"))
    )
    invalid = (
        F.col("product_id").isNull()
        | F.col("unit_price").isNull()
        | (F.col("unit_price") < 0)
        | F.col("category").isNull()
    )
    return _split_valid_rejected(cleaned, invalid, "invalid_product_record", "product_id")


def clean_sales(
    df: DataFrame,
    customers: DataFrame,
    products: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    cleaned = (
        _clean_strings(df)
        .withColumn("sale_id", F.col("sale_id").cast("int"))
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("product_id", F.col("product_id").cast("int"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(12,2)"))
        .withColumn("discount_pct", F.col("discount_pct").cast("decimal(5,2)"))
        .withColumn("total_amount", F.col("total_amount").cast("decimal(14,2)"))
    )
    customer_dates = customers.select("customer_id", "signup_date")
    valid_sales = (
        cleaned.join(F.broadcast(customer_dates), "customer_id", "left")
        .join(
            F.broadcast(products.select("product_id").withColumn("_product_exists", F.lit(True))),
            "product_id",
            "left",
        )
        .withColumn(
            "_expected_amount",
            (
                F.col("unit_price")
                * F.col("quantity")
                * (F.lit(1) - F.col("discount_pct") / F.lit(100))
            ).cast("decimal(14,2)"),
        )
    )
    invalid = (
        F.col("sale_id").isNull()
        | F.col("order_date").isNull()
        | F.col("customer_id").isNull()
        | F.col("product_id").isNull()
        | F.col("quantity").isNull()
        | (F.col("quantity") <= 0)
        | F.col("unit_price").isNull()
        | (F.col("unit_price") < 0)
        | F.col("discount_pct").isNull()
        | (F.col("discount_pct") < 0)
        | (F.col("discount_pct") > 100)
        | F.col("total_amount").isNull()
        | (F.abs(F.col("total_amount") - F.col("_expected_amount")) > F.lit(0.01))
        | ~F.col("sales_channel").isin(*VALID_CHANNELS)
        | ~F.col("payment_method").isin(*VALID_PAYMENTS)
        | ~F.col("order_status").isin(*VALID_STATUSES)
        | F.col("signup_date").isNull()
        | (F.col("order_date") < F.col("signup_date"))
        | F.col("_product_exists").isNull()
    )
    valid_sales = valid_sales.drop("signup_date", "_expected_amount", "_product_exists")
    accepted, rejected = _split_valid_rejected(
        valid_sales, invalid, "invalid_sales_record", "sale_id"
    )
    return accepted, rejected


def _split_valid_rejected(
    df: DataFrame,
    invalid_condition: Column,
    reason: str,
    key_column: str,
) -> tuple[DataFrame, DataFrame]:
    ranked = df.withColumn(
        "_row_number",
        F.row_number().over(
            Window.partitionBy(key_column).orderBy(F.col("_ingested_at").desc_nulls_last())
        ),
    )
    duplicate = F.col("_row_number") > 1
    rejected = (
        ranked.filter(invalid_condition | duplicate)
        .withColumn(
            "_rejection_reason",
            F.when(duplicate, F.lit("duplicate_key")).otherwise(F.lit(reason)),
        )
        .drop("_row_number")
    )
    accepted = ranked.filter(~invalid_condition & ~duplicate).drop("_row_number")
    return accepted, rejected

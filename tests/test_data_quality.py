"""Data quality validation tests for retail-customer-360."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                                StructField, StructType)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .appName("dq-test-retail")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def null_rate(df, column: str) -> float:
    total = df.count()
    if total == 0:
        return 0.0
    nulls = df.filter(F.col(column).isNull()).count()
    return nulls / total * 100


def duplicate_rate(df, key_column: str) -> float:
    total = df.count()
    if total == 0:
        return 0.0
    distinct = df.select(key_column).distinct().count()
    return (total - distinct) / total * 100


class TestCustomerDataQuality:
    @pytest.fixture
    def sample_c360(self, spark):
        schema = StructType([
            StructField("customer_id",      StringType()),
            StructField("full_name",         StringType()),
            StructField("email",             StringType()),
            StructField("country",           StringType()),
            StructField("total_orders",      IntegerType()),
            StructField("total_spend",       DoubleType()),
            StructField("customer_segment",  StringType()),
        ])
        data = [
            ("C001", "Alice Smith",   "alice@example.com", "US", 5, 500.0, "VIP"),
            ("C002", "Bob Jones",     "bob@example.com",   "UK", 2, 120.0, "Regular"),
            ("C003", "Carol White",   "carol@example.com", "AU", 1, 45.0,  "New"),
            ("C004", "David Brown",   "david@example.com", "CA", 0, 0.0,   "No Orders"),
        ]
        return spark.createDataFrame(data, schema)

    def test_customer_id_null_rate_below_threshold(self, sample_c360):
        rate = null_rate(sample_c360, "customer_id")
        assert rate == 0.0, f"customer_id null rate {rate:.1f}% exceeds 0%"

    def test_email_null_rate_below_threshold(self, sample_c360):
        rate = null_rate(sample_c360, "email")
        assert rate < 5.0, f"email null rate {rate:.1f}% exceeds 5%"

    def test_customer_id_uniqueness(self, sample_c360):
        dup_rate = duplicate_rate(sample_c360, "customer_id")
        assert dup_rate == 0.0, f"Duplicate customer_ids detected: {dup_rate:.1f}%"

    def test_total_spend_non_negative(self, sample_c360):
        negative_count = sample_c360.filter(F.col("total_spend") < 0).count()
        assert negative_count == 0, f"{negative_count} rows have negative total_spend"

    def test_valid_customer_segments(self, sample_c360):
        valid = {"VIP", "Regular", "New", "At-Risk", "No Orders"}
        actual = {r["customer_segment"] for r in sample_c360.select("customer_segment").distinct().collect()}
        invalid = actual - valid
        assert not invalid, f"Invalid segments found: {invalid}"


class TestOrderDataQuality:
    @pytest.fixture
    def sample_orders(self, spark):
        schema = StructType([
            StructField("order_id",    StringType()),
            StructField("customer_id", StringType()),
            StructField("quantity",    IntegerType()),
            StructField("total_amount",DoubleType()),
            StructField("status",      StringType()),
        ])
        data = [
            ("O001", "C001", 2, 90.0,  "completed"),
            ("O002", "C001", 1, 45.0,  "completed"),
            ("O003", "C002", 3, 120.0, "completed"),
            ("O004", "C003", 1, 20.0,  "returned"),
        ]
        return spark.createDataFrame(data, schema)

    def test_order_id_uniqueness(self, sample_orders):
        assert duplicate_rate(sample_orders, "order_id") == 0.0

    def test_no_negative_totals(self, sample_orders):
        neg = sample_orders.filter(F.col("total_amount") < 0).count()
        assert neg == 0

    def test_all_quantities_positive(self, sample_orders):
        non_pos = sample_orders.filter(F.col("quantity") <= 0).count()
        assert non_pos == 0

    def test_valid_order_statuses(self, sample_orders):
        valid = {"completed", "returned", "cancelled", "pending"}
        actual = {r["status"] for r in sample_orders.select("status").distinct().collect()}
        assert actual.issubset(valid), f"Invalid statuses: {actual - valid}"

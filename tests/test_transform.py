"""Unit tests for transform.py"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                                StructField, StructType)

from etl.transform import CustomerTransformer, OrderTransformer


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .appName("test-retail-customer-360")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def customer_transformer(spark):
    return CustomerTransformer(spark)


@pytest.fixture
def order_transformer(spark):
    return OrderTransformer(spark)


@pytest.fixture
def raw_customers(spark):
    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("first_name",  StringType()),
        StructField("last_name",   StringType()),
        StructField("email",       StringType()),
        StructField("country",     StringType()),
        StructField("city",        StringType()),
        StructField("phone",       StringType()),
        StructField("registration_date", StringType()),
        StructField("is_active",   StringType()),
        StructField("created_at",  StringType()),
        StructField("updated_at",  StringType()),
    ])
    data = [
        ("CUST001", " Alice ", "Smith", "ALICE@EXAMPLE.COM", "US", "NYC", "+1-555-1234", "2022-01-01", "true",  "2022-01-01 00:00:00", "2023-01-01 00:00:00"),
        ("CUST002", "Bob",     "Jones", "bob@example.com",   "UK", "London", "+44-20-1234", "2021-06-01", "1", "2021-06-01 00:00:00", "2022-06-01 00:00:00"),
        ("CUST001", " Alice ", "Smith", "ALICE@EXAMPLE.COM", "US", "NYC", "+1-555-1234", "2022-01-01", "true",  "2022-01-01 00:00:00", "2024-01-01 00:00:00"),  # duplicate newer
        (None,      "Ghost",   "User",  "ghost@example.com", "US", "NYC", "",             "2022-01-01", "false", "2022-01-01 00:00:00", "2022-01-01 00:00:00"),  # null id
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def raw_orders(spark):
    schema = StructType([
        StructField("order_id",     StringType()),
        StructField("customer_id",  StringType()),
        StructField("product_id",   StringType()),
        StructField("quantity",     IntegerType()),
        StructField("unit_price",   DoubleType()),
        StructField("discount_pct", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("currency",     StringType()),
        StructField("status",       StringType()),
        StructField("channel",      StringType()),
        StructField("order_date",   StringType()),
        StructField("shipped_date", StringType()),
        StructField("country",      StringType()),
        StructField("created_at",   StringType()),
        StructField("updated_at",   StringType()),
    ])
    data = [
        ("ORD001", "CUST001", "PROD001", 2, 50.0, 0.1, 90.0,  "USD", "Completed", "web",   "2023-03-01", "2023-03-03", "US", "2023-03-01 00:00:00", "2023-03-01 00:00:00"),
        ("ORD002", "CUST001", "PROD002", 1, 25.0, 0.0, 25.0,  "USD", "completed", "mobile","2023-06-01", "2023-06-02", "US", "2023-06-01 00:00:00", "2023-06-01 00:00:00"),
        ("ORD003", "CUST002", "PROD001", 3, 50.0, 0.0, 150.0, "USD", "completed", "web",   "2022-12-01", "2022-12-03", "UK", "2022-12-01 00:00:00", "2022-12-01 00:00:00"),
        ("ORD004", None,      "PROD001", 1, 10.0, 0.0, 10.0,  "USD", "completed", "web",   "2023-01-01", "2023-01-02", "US", "2023-01-01 00:00:00", "2023-01-01 00:00:00"),  # null customer
        ("ORD005", "CUST001", "PROD003", -1, 20.0, 0.0, -20.0,"USD", "completed", "store", "2023-07-01", "2023-07-01", "US", "2023-07-01 00:00:00", "2023-07-01 00:00:00"),  # negative qty
    ]
    return spark.createDataFrame(data, schema)


# ------------------------------------------------------------------
# Customer cleaning tests
# ------------------------------------------------------------------

class TestCleanCustomers:
    def test_removes_null_customer_ids(self, customer_transformer, raw_customers):
        result = customer_transformer.clean_customers(raw_customers)
        ids = [r["customer_id"] for r in result.collect()]
        assert None not in ids

    def test_deduplicates_customers(self, customer_transformer, raw_customers):
        result = customer_transformer.clean_customers(raw_customers)
        assert result.count() == 2  # CUST001 (latest) + CUST002

    def test_normalises_email_to_lowercase(self, customer_transformer, raw_customers):
        result = customer_transformer.clean_customers(raw_customers)
        emails = [r["email"] for r in result.collect()]
        assert all(e == e.lower() for e in emails)

    def test_trims_whitespace_from_first_name(self, customer_transformer, raw_customers):
        result = customer_transformer.clean_customers(raw_customers)
        alice = result.filter(result.customer_id == "CUST001").first()
        assert alice["first_name"] == "Alice"


# ------------------------------------------------------------------
# Order cleaning tests
# ------------------------------------------------------------------

class TestCleanOrders:
    def test_removes_null_customer_id_orders(self, customer_transformer, raw_orders):
        result = customer_transformer.clean_orders(raw_orders)
        cids = [r["customer_id"] for r in result.collect()]
        assert None not in cids

    def test_removes_negative_quantity_orders(self, customer_transformer, raw_orders):
        result = customer_transformer.clean_orders(raw_orders)
        qtys = [r["quantity"] for r in result.collect()]
        assert all(q > 0 for q in qtys)

    def test_normalises_status_to_lowercase(self, customer_transformer, raw_orders):
        result = customer_transformer.clean_orders(raw_orders)
        statuses = [r["status"] for r in result.collect()]
        assert all(s == s.lower() for s in statuses)


# ------------------------------------------------------------------
# Customer 360 build tests
# ------------------------------------------------------------------

class TestBuildCustomer360:
    def test_customer_360_has_expected_columns(self, customer_transformer, raw_customers, raw_orders):
        customers = customer_transformer.clean_customers(raw_customers)
        orders    = customer_transformer.clean_orders(raw_orders)
        c360      = customer_transformer.build_customer_360(customers, orders)
        expected_cols = {"customer_id", "full_name", "total_orders", "total_spend",
                         "avg_order_value", "customer_segment"}
        assert expected_cols.issubset(set(c360.columns))

    def test_vip_segment_assigned_correctly(self, spark, customer_transformer):
        schema = StructType([
            StructField("total_spend", DoubleType()),
            StructField("total_orders", IntegerType()),
            StructField("days_since_last_order", IntegerType()),
        ])
        df = spark.createDataFrame([(1500.0, 10, 5)], schema)
        result = customer_transformer.assign_customer_segment(df)
        assert result.first()["customer_segment"] == "VIP"

    def test_at_risk_segment_assigned_for_old_orders(self, spark, customer_transformer):
        schema = StructType([
            StructField("total_spend", DoubleType()),
            StructField("total_orders", IntegerType()),
            StructField("days_since_last_order", IntegerType()),
        ])
        df = spark.createDataFrame([(200.0, 3, 200)], schema)
        result = customer_transformer.assign_customer_segment(df)
        assert result.first()["customer_segment"] == "At-Risk"

"""
PySpark transformation layer for retail-customer-360.

Cleans, enriches, and aggregates raw customer and order data
into a unified Customer 360 view.
"""

from typing import Optional

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


class CustomerTransformer:
    """Transforms raw customer records into enriched, analytics-ready format."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def clean_customers(self, df: DataFrame) -> DataFrame:
        """Standardise and clean raw customer data.

        - Trims whitespace from string columns
        - Normalises email to lowercase
        - Casts is_active to boolean
        - Drops duplicate customer_id records (keeps latest updated_at)

        Args:
            df: Raw customers DataFrame.

        Returns:
            Cleaned DataFrame.
        """
        logger.info("Cleaning customer records...")

        string_cols = ["first_name", "last_name", "email", "country", "city", "phone"]
        for col in string_cols:
            if col in df.columns:
                df = df.withColumn(col, F.trim(F.col(col)))

        df = df.withColumn("email", F.lower(F.col("email")))

        if "is_active" in df.columns:
            df = df.withColumn(
                "is_active",
                F.when(F.col("is_active").cast("string").isin("true", "1", "yes", "True"), True)
                .otherwise(False)
            )

        # Deduplicate: keep the row with the latest updated_at per customer_id
        win = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())
        df = (
            df.withColumn("_row_num", F.row_number().over(win))
              .filter(F.col("_row_num") == 1)
              .drop("_row_num")
        )

        logger.info(f"  Clean customer count: {df.count():,}")
        return df

    def clean_orders(self, df: DataFrame) -> DataFrame:
        """Standardise and clean raw order data.

        - Enforces non-negative quantity and total_amount
        - Normalises order status to lowercase
        - Removes orders with null customer_id or product_id

        Args:
            df: Raw orders DataFrame.

        Returns:
            Cleaned DataFrame.
        """
        logger.info("Cleaning order records...")

        df = df.filter(F.col("customer_id").isNotNull() & F.col("product_id").isNotNull())
        df = df.withColumn("status", F.lower(F.trim(F.col("status"))))
        df = df.filter((F.col("quantity") > 0) & (F.col("total_amount") >= 0))

        logger.info(f"  Clean order count: {df.count():,}")
        return df

    def build_customer_metrics(self, orders_df: DataFrame) -> DataFrame:
        """Aggregate order-level metrics per customer.

        Computes: total_orders, total_spend, avg_order_value, first_seen,
        last_seen, days_since_last_order, and preferred_channel.

        Args:
            orders_df: Cleaned orders DataFrame.

        Returns:
            DataFrame keyed by customer_id with computed metrics.
        """
        logger.info("Building customer metrics from orders...")

        completed = orders_df.filter(F.col("status") == "completed")

        metrics = completed.groupBy("customer_id").agg(
            F.count("order_id").alias("total_orders"),
            F.round(F.sum("total_amount"), 2).alias("total_spend"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )

        metrics = metrics.withColumn(
            "days_since_last_order",
            F.datediff(F.current_date(), F.col("last_order_date"))
        )

        # Preferred channel: mode per customer
        channel_counts = (
            completed.groupBy("customer_id", "channel")
            .agg(F.count("*").alias("cnt"))
        )
        win = Window.partitionBy("customer_id").orderBy(F.col("cnt").desc())
        preferred_channel = (
            channel_counts.withColumn("_rn", F.row_number().over(win))
            .filter(F.col("_rn") == 1)
            .select("customer_id", F.col("channel").alias("preferred_channel"))
        )

        metrics = metrics.join(preferred_channel, on="customer_id", how="left")
        logger.info(f"  Customer metrics rows: {metrics.count():,}")
        return metrics

    def assign_customer_segment(self, df: DataFrame) -> DataFrame:
        """Segment customers based on RFM-like rules.

        Segments:
          - VIP: total_spend >= 1000 AND total_orders >= 5
          - At-Risk: days_since_last_order > 180
          - New: total_orders == 1
          - Regular: everything else

        Args:
            df: DataFrame containing total_spend, total_orders,
                days_since_last_order columns.

        Returns:
            DataFrame with added customer_segment column.
        """
        logger.info("Assigning customer segments...")
        df = df.withColumn(
            "customer_segment",
            F.when(
                (F.col("total_spend") >= 1000) & (F.col("total_orders") >= 5), "VIP"
            ).when(
                F.col("days_since_last_order") > 180, "At-Risk"
            ).when(
                F.col("total_orders") == 1, "New"
            ).otherwise("Regular")
        )
        return df

    def build_customer_360(
        self,
        customers_df: DataFrame,
        orders_df: DataFrame,
    ) -> DataFrame:
        """Join customer profiles with order metrics to produce Customer 360.

        Args:
            customers_df: Cleaned customer records.
            orders_df: Cleaned order records.

        Returns:
            Customer 360 DataFrame ready for loading into the mart.
        """
        logger.info("Building Customer 360 view...")

        metrics = self.build_customer_metrics(orders_df)
        c360 = customers_df.join(metrics, on="customer_id", how="left")
        c360 = c360.fillna({"total_orders": 0, "total_spend": 0.0, "avg_order_value": 0.0})
        c360 = self.assign_customer_segment(c360)

        c360 = c360.select(
            "customer_id",
            F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("full_name"),
            "email",
            "country",
            "city",
            F.col("registration_date").alias("registered_on"),
            "is_active",
            "total_orders",
            "total_spend",
            "avg_order_value",
            "first_order_date",
            "last_order_date",
            "days_since_last_order",
            "preferred_channel",
            "customer_segment",
            F.current_timestamp().alias("last_refreshed_at"),
        )

        logger.info(f"  Customer 360 rows: {c360.count():,}")
        return c360


class OrderTransformer:
    """Transforms and aggregates order data for the order summary mart."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def build_order_summary(self, orders_df: DataFrame, products_df: Optional[DataFrame] = None) -> DataFrame:
        """Build daily order summary by country and category.

        Args:
            orders_df: Cleaned orders DataFrame.
            products_df: Optional products DataFrame for category enrichment.

        Returns:
            Aggregated order summary DataFrame.
        """
        logger.info("Building order summary...")
        df = orders_df.filter(F.col("status") == "completed")

        if products_df is not None:
            df = df.join(products_df.select("product_id", "category"), on="product_id", how="left")
        else:
            df = df.withColumn("category", F.lit("Unknown"))

        summary = df.groupBy(
            F.col("order_date").alias("report_date"),
            "country",
            "channel",
            "category",
        ).agg(
            F.count("order_id").alias("total_orders"),
            F.sum("quantity").alias("total_units_sold"),
            F.round(F.sum("total_amount"), 2).alias("gross_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
            F.round(F.avg("discount_pct") * 100, 2).alias("avg_discount_pct"),
        ).orderBy("report_date", "country")

        logger.info(f"  Order summary rows: {summary.count():,}")
        return summary

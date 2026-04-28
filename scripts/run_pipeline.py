"""
Pipeline runner for retail-customer-360.

Usage:
    python run_pipeline.py --mode sample
    python run_pipeline.py --mode incremental --run-date 2024-06-01
    python run_pipeline.py --mode full
"""

import argparse
import os
import sys
from pathlib import Path

import yaml
from loguru import logger
from pyspark.sql import SparkSession

# Add project root to path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from etl.extract import DataExtractor
from etl.transform import CustomerTransformer, OrderTransformer
from etl.load import SnowflakeLoader


def load_config(path: str = None) -> dict:
    config_path = path or os.path.join(ROOT, "config", "config.yaml")
    with open(config_path) as f:
        raw = f.read()
    # Substitute environment variables
    import re
    def replace_env(m):
        key = m.group(1)
        return os.environ.get(key, m.group(0))
    raw = re.sub(r"\$\{(\w+)\}", replace_env, raw)
    return yaml.safe_load(raw)


def build_spark(config: dict) -> SparkSession:
    spark_cfg = config.get("spark", {})
    builder = SparkSession.builder.appName(spark_cfg.get("app_name", "retail-customer-360"))
    builder = builder.master(spark_cfg.get("master", "local[*]"))
    for key, val in spark_cfg.get("config", {}).items():
        builder = builder.config(key, val)
    return builder.getOrCreate()


def run_sample_mode(spark: SparkSession, config: dict) -> None:
    """Run pipeline using local CSV sample data (no Snowflake required)."""
    data_dir = os.path.join(ROOT, "data", "sample")

    # Check sample data exists
    for fname in ["customers.csv", "orders.csv", "products.csv"]:
        if not os.path.exists(os.path.join(data_dir, fname)):
            logger.warning(f"Sample file not found: {fname}. Running generate_sample_data.py first...")
            import subprocess
            subprocess.run([sys.executable, os.path.join(ROOT, "scripts", "generate_sample_data.py")], check=True)
            break

    extractor = DataExtractor(spark, config)
    transformer_c = CustomerTransformer(spark)
    transformer_o = OrderTransformer(spark)

    logger.info("=== EXTRACT ===")
    customers_raw = extractor.read_csv(os.path.join(data_dir, "customers.csv"))
    orders_raw    = extractor.read_csv(os.path.join(data_dir, "orders.csv"))
    products_raw  = extractor.read_csv(os.path.join(data_dir, "products.csv"))

    extractor.validate_not_empty(customers_raw, "customers")
    extractor.validate_not_empty(orders_raw, "orders")

    logger.info("=== TRANSFORM ===")
    customers_clean = transformer_c.clean_customers(customers_raw)
    orders_clean    = transformer_c.clean_orders(orders_raw)
    c360            = transformer_c.build_customer_360(customers_clean, orders_clean)
    order_summary   = transformer_o.build_order_summary(orders_clean, products_raw)

    logger.info("=== LOAD (sample mode — writing Parquet locally) ===")
    out_dir = os.path.join(ROOT, "data", "output")
    os.makedirs(out_dir, exist_ok=True)

    c360.coalesce(1).write.mode("overwrite").parquet(os.path.join(out_dir, "customer_360"))
    order_summary.coalesce(1).write.mode("overwrite").parquet(os.path.join(out_dir, "order_summary"))

    logger.info("Pipeline complete (sample mode).")
    logger.info(f"  Customer 360 rows: {c360.count():,}")
    logger.info(f"  Order summary rows: {order_summary.count():,}")

    # Print sample
    logger.info("\n--- Customer 360 Sample ---")
    c360.show(5, truncate=False)


def run_production_mode(spark: SparkSession, config: dict, incremental: bool, run_date: str) -> None:
    """Run pipeline in production mode connecting to Snowflake."""
    watermark_col = config["pipeline"].get("watermark_column", "updated_at")

    extractor   = DataExtractor(spark, config)
    transformer_c = CustomerTransformer(spark)
    transformer_o = OrderTransformer(spark)
    loader      = SnowflakeLoader(spark, config)

    logger.info(f"=== EXTRACT (incremental={incremental}, run_date={run_date}) ===")
    last_wm = f"{run_date} 00:00:00" if incremental else None
    customers_raw = extractor.read_snowflake_table("CUSTOMERS", watermark_col if incremental else None, last_wm)
    orders_raw    = extractor.read_snowflake_table("ORDERS", watermark_col if incremental else None, last_wm)
    products_raw  = extractor.read_snowflake_table("PRODUCTS")

    logger.info("=== TRANSFORM ===")
    customers_clean = transformer_c.clean_customers(customers_raw)
    orders_clean    = transformer_c.clean_orders(orders_raw)
    c360            = transformer_c.build_customer_360(customers_clean, orders_clean)
    order_summary   = transformer_o.build_order_summary(orders_clean, products_raw)

    logger.info("=== LOAD ===")
    schema_marts = config["snowflake"]["schemas"]["marts"]
    loader.write_merge(c360, schema_marts, "CUSTOMER_360", ["customer_id"])
    loader.write_merge(order_summary, schema_marts, "ORDER_SUMMARY",
                       ["report_date", "country", "channel", "category"])
    logger.info("Pipeline complete.")


def main():
    parser = argparse.ArgumentParser(description="retail-customer-360 pipeline runner")
    parser.add_argument("--mode", choices=["sample", "full", "incremental"], default="sample")
    parser.add_argument("--run-date", default=None, help="Run date (YYYY-MM-DD) for incremental mode")
    parser.add_argument("--config", default=None, help="Path to config file")
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stdout, level="INFO", colorize=True,
               format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}")

    config = load_config(args.config)
    spark  = build_spark(config)
    spark.sparkContext.setLogLevel("WARN")

    try:
        if args.mode == "sample":
            run_sample_mode(spark, config)
        else:
            from datetime import date
            run_date = args.run_date or str(date.today())
            run_production_mode(spark, config, incremental=(args.mode == "incremental"), run_date=run_date)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

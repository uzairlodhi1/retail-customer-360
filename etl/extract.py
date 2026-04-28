"""
Extraction layer for retail-customer-360 pipeline.

Supports reading from:
  - Local CSV files (sample/dev mode)
  - Snowflake raw tables (staging/production)
  - S3 buckets (batch ingestion)
"""

import os
from pathlib import Path
from typing import Optional

import pandas as pd
from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


class DataExtractor:
    """Extracts raw data from configured sources."""

    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.pipeline_cfg = config.get("pipeline", {})

    # ------------------------------------------------------------------
    # CSV / local file extraction
    # ------------------------------------------------------------------

    def read_csv(
        self,
        path: str,
        schema: Optional[StructType] = None,
        header: bool = True,
    ) -> DataFrame:
        """Read a CSV file into a Spark DataFrame.

        Args:
            path: Absolute or relative path to the CSV file.
            schema: Optional explicit schema; inferred if None.
            header: Whether the first row is a header.

        Returns:
            Spark DataFrame with raw data.
        """
        logger.info(f"Reading CSV: {path}")
        reader = self.spark.read.option("header", str(header).lower())

        if schema:
            reader = reader.schema(schema)
        else:
            reader = reader.option("inferSchema", "true")

        df = reader.csv(path)
        logger.info(f"  Rows loaded: {df.count():,}  |  Columns: {len(df.columns)}")
        return df

    # ------------------------------------------------------------------
    # Snowflake extraction
    # ------------------------------------------------------------------

    def read_snowflake_table(
        self,
        table: str,
        watermark_column: Optional[str] = None,
        last_watermark: Optional[str] = None,
    ) -> DataFrame:
        """Read a Snowflake table, optionally filtered by watermark for incremental loads.

        Args:
            table: Fully qualified table name (schema.table_name).
            watermark_column: Column used for incremental filtering.
            last_watermark: Last processed watermark value (ISO timestamp string).

        Returns:
            Spark DataFrame.
        """
        sf_cfg = self.config["snowflake"]
        options = {
            "sfURL": f"{sf_cfg['account']}.snowflakecomputing.com",
            "sfUser": sf_cfg["user"],
            "sfPassword": sf_cfg["password"],
            "sfDatabase": sf_cfg["database"],
            "sfWarehouse": sf_cfg["warehouse"],
            "sfSchema": sf_cfg["schemas"]["raw"],
            "dbtable": table,
        }

        if watermark_column and last_watermark:
            query = f"SELECT * FROM {table} WHERE {watermark_column} > '{last_watermark}'"
            options["query"] = query
            del options["dbtable"]
            logger.info(f"Incremental read: {table} where {watermark_column} > {last_watermark}")
        else:
            logger.info(f"Full read: {table}")

        df = (
            self.spark.read.format("net.snowflake.spark.snowflake")
            .options(**options)
            .load()
        )
        logger.info(f"  Rows loaded: {df.count():,}")
        return df

    # ------------------------------------------------------------------
    # S3 extraction
    # ------------------------------------------------------------------

    def read_s3_parquet(self, s3_path: str) -> DataFrame:
        """Read Parquet files from an S3 path.

        Args:
            s3_path: S3 URI, e.g. s3://bucket/prefix/

        Returns:
            Spark DataFrame.
        """
        logger.info(f"Reading S3 Parquet: {s3_path}")
        df = self.spark.read.parquet(s3_path)
        logger.info(f"  Rows loaded: {df.count():,}  |  Partitions: {df.rdd.getNumPartitions()}")
        return df

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def add_pipeline_metadata(self, df: DataFrame, source: str) -> DataFrame:
        """Append audit columns to a DataFrame.

        Args:
            df: Input Spark DataFrame.
            source: Source system identifier string.

        Returns:
            DataFrame with added _source, _ingested_at, _pipeline_run_id columns.
        """
        import uuid
        run_id = str(uuid.uuid4())
        return df.withColumn("_source", F.lit(source)) \
                 .withColumn("_ingested_at", F.current_timestamp()) \
                 .withColumn("_pipeline_run_id", F.lit(run_id))

    def validate_not_empty(self, df: DataFrame, name: str) -> None:
        """Raise ValueError if DataFrame is empty."""
        count = df.count()
        if count == 0:
            raise ValueError(f"Extraction produced 0 rows for '{name}'. Aborting pipeline.")
        logger.info(f"Validation passed: '{name}' has {count:,} rows.")

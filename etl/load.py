"""
Snowflake load utilities for retail-customer-360.

Supports:
  - Full overwrite loads
  - Merge (upsert) using Snowflake MERGE INTO
  - Partition-aware writes
  - Write-audit-publish pattern
"""

from typing import Optional

from loguru import logger
from pyspark.sql import DataFrame, SparkSession


class SnowflakeLoader:
    """Loads Spark DataFrames into Snowflake tables."""

    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        sf_cfg = config["snowflake"]
        self.sf_options = {
            "sfURL": f"{sf_cfg['account']}.snowflakecomputing.com",
            "sfUser": sf_cfg["user"],
            "sfPassword": sf_cfg["password"],
            "sfDatabase": sf_cfg["database"],
            "sfWarehouse": sf_cfg["warehouse"],
            "sfRole": sf_cfg.get("role", "DATA_ENGINEER"),
        }

    def write_overwrite(self, df: DataFrame, schema: str, table: str) -> None:
        """Truncate and reload a Snowflake table.

        Args:
            df: DataFrame to write.
            schema: Target Snowflake schema.
            table: Target table name.
        """
        full_table = f"{schema}.{table}"
        logger.info(f"OVERWRITE → {full_table} ({df.count():,} rows)")

        options = {**self.sf_options, "sfSchema": schema, "dbtable": full_table}
        df.write.format("net.snowflake.spark.snowflake") \
            .options(**options) \
            .mode("overwrite") \
            .save()

        logger.info(f"Load complete: {full_table}")

    def write_append(self, df: DataFrame, schema: str, table: str) -> None:
        """Append rows to an existing Snowflake table.

        Args:
            df: DataFrame to append.
            schema: Target Snowflake schema.
            table: Target table name.
        """
        full_table = f"{schema}.{table}"
        logger.info(f"APPEND → {full_table} ({df.count():,} rows)")

        options = {**self.sf_options, "sfSchema": schema, "dbtable": full_table}
        df.write.format("net.snowflake.spark.snowflake") \
            .options(**options) \
            .mode("append") \
            .save()

        logger.info(f"Append complete: {full_table}")

    def write_merge(
        self,
        df: DataFrame,
        schema: str,
        table: str,
        merge_keys: list[str],
        staging_table: Optional[str] = None,
    ) -> None:
        """Upsert (MERGE INTO) from a staging table into the target.

        Pattern: write to staging → MERGE staging into target → drop staging.

        Args:
            df: DataFrame containing new/updated records.
            schema: Target Snowflake schema.
            table: Target table name.
            merge_keys: List of columns to match on.
            staging_table: Override for staging table name.
        """
        staging = staging_table or f"{table}_STAGING_{_run_id()}"
        full_target = f"{schema}.{table}"
        full_staging = f"{schema}.{staging}"

        logger.info(f"MERGE → {full_target}  (keys: {merge_keys})")

        # Step 1: write to staging
        self.write_overwrite(df, schema, staging)

        # Step 2: build MERGE SQL
        match_clause = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])
        all_cols = [c for c in df.columns if c not in merge_keys]
        update_set = ", ".join([f"t.{c} = s.{c}" for c in all_cols])
        insert_cols = ", ".join(df.columns)
        insert_vals = ", ".join([f"s.{c}" for c in df.columns])

        merge_sql = f"""
            MERGE INTO {full_target} t
            USING {full_staging} s
            ON {match_clause}
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols}) VALUES ({insert_vals})
        """

        logger.debug(f"Executing MERGE SQL:\n{merge_sql}")
        self._execute_sql(merge_sql, schema)

        # Step 3: drop staging
        self._execute_sql(f"DROP TABLE IF EXISTS {full_staging}", schema)
        logger.info(f"Merge complete: {full_target}")

    def _execute_sql(self, sql: str, schema: str) -> None:
        """Execute arbitrary SQL via Snowflake Spark connector."""
        options = {**self.sf_options, "sfSchema": schema, "query": sql}
        # Use an empty DataFrame as a vehicle for SQL execution
        self.spark.read.format("net.snowflake.spark.snowflake") \
            .options(**options) \
            .load()

    def record_row_counts(self, schema: str, tables: list[str]) -> dict:
        """Return a dict of {table: row_count} for audit logging."""
        counts = {}
        for table in tables:
            result = self._query_to_df(f"SELECT COUNT(*) AS cnt FROM {schema}.{table}", schema)
            counts[table] = result.collect()[0]["CNT"]
            logger.info(f"  {schema}.{table}: {counts[table]:,} rows")
        return counts

    def _query_to_df(self, query: str, schema: str) -> "DataFrame":
        options = {**self.sf_options, "sfSchema": schema, "query": query}
        return self.spark.read.format("net.snowflake.spark.snowflake").options(**options).load()


def _run_id() -> str:
    import uuid
    return uuid.uuid4().hex[:8].upper()

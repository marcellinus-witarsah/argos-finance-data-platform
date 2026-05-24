# ================================================================================
# Author: Marcellinus A.W.
# Date: 16 May 2026
# Description: Decided to create an abstract class for extractor so that each new
# extractor has to inherite from this base class
# ================================================================================

from src.writer.base_writer import BaseWriter
from pyspark.sql import DataFrame

class IcebergWriter(BaseWriter):
    def write(self, df: DataFrame) -> DataFrame:
        mode = self.cfg.get("mode", "")
        catalog = self.cfg.get("catalog", "")
        schema = self.cfg.get("schema", "")
        table = self.cfg.get("table", "")
        if mode == "append":
            self._write_append(df, f"{catalog}.{schema}.{table}")
        elif mode == "merge":
            self._write_merge(df, f"{catalog}.{schema}.{table}")

    def _write_append(self, df: DataFrame, table: str):
        if self._is_table_exists(table):
            writer = df.writeTo(table).using("iceberg")
            writer = self._apply_partition(writer)
            self._evolve_schema(df, table)
            writer.append()
            self.ctx.logger.info(f"Append Spark DataFrame records into {table} table.")
        else:
            self._create_table(df, table)

    def _write_merge(self, df: DataFrame, table: str):
        if self._is_table_exists(table):
            df.createOrReplaceTempView("updates")
            merge_columns = self.cfg.get("merge_column", [])
            sql = f"""
                MERGE INTO {table} AS t
                USING updates AS s
                ON {" AND ".join([f"t.{column}=s.{column}" for column in merge_columns])}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """
            self.ctx.spark.sql(sql)
            self.ctx.logger.info(f"Upsert Spark DataFrame records into {table} table.")
        else:
            self._create_table(df, table)
    
    def _create_table(self, df: DataFrame, table: str):
        writer = df.writeTo(table).using("iceberg")
        writer = self._apply_partition(writer)
        writer.create()
        self.ctx.logger.info(f"Table {table} doesn't exist.")
        self.ctx.logger.info(f"Create a new {table} table from Spark DataFrame records.")

    def _evolve_schema(self, df: DataFrame, table: str):
        existing_cols = set(self.ctx.spark.table(table).columns)
        for field in df.schema.fields:
            if field.name not in existing_cols:
                self.ctx.spark.sql(
                    f"ALTER TABLE {table} ADD COLUMN {field.name} {field.dataType.simpleString()}"
                )
                self.ctx.logger.info(
                    f"Add a new `{field.name}` column into `{table}` table."
                )

    def _apply_partition(self, writer):
        partition_by = self.cfg.get("partition_by", "")
        if partition_by:
            writer.partitionedBy(partition_by)
            self.ctx.logger.info(
                f"Apply partition to `{self.cfg.get('table', '')}` table by `{self.cfg.get('partition_by', '')}` column."
            )
        return writer

    def _is_table_exists(self, table: str) -> bool:
        return self.ctx.spark.catalog.tableExists(table)

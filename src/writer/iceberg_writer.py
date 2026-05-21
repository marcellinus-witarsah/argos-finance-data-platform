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
        mode = self.cfg["mode"]
        table = self.cfg["table"]
        if mode == "append":
            self._write_append(df, table)
        elif mode == "merge":
            self._write_merge(df, table)
    
    def _write_append(self, df: DataFrame, table: str):
        writer = df.writeTo(table).using("iceberg")
        self.ctx.logger.info("success creating writer for inputing into iceberg")
        writer = self._apply_partition(writer)
        if self._is_table_exists(table):
            self._evolve_schema(df, table)
            writer.append()
        else:
            writer.create()

    def _write_merge(self, df: DataFrame, table: str):
        pass

    def _evolve_schema(self, df: DataFrame, table: str):
        existing_cols = set(self.ctx.spark.table(table).columns)
        for field in df.schema.fields:
            if field.name not in existing_cols:
                self.ctx.spark.sql(
                    f"ALTER TABLE {table} ADD COLUMN {field.name} {field.dataType.simpleString()}"
                )
                self.ctx.logger.info(f"added column {field.name} to {table}")

    def _apply_partition(self, writer):
        partition_by = self.cfg["partition_by"]
        if partition_by:
            writer.partitionedBy(partition_by)
        return writer


    def _is_table_exists(self, table: str) -> bool:
        return self.ctx.spark.catalog.tableExists(table)

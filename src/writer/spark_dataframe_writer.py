# ================================================================================
# Author: Marcellinus A.W.
# Date: 16 May 2026
# Description: Decided to create an abstract class for extractor so that each new
# extractor has to inherite from this base class
# ================================================================================

from src.writer.base_writer import BaseWriter
from pyspark.sql import SparkSession, DataFrame, DataFrameWriterV2
from src.utils.logger import logger
from typing import Optional

class SparkDataframeWriter(BaseWriter):

    FORMATS = ["iceberg", "delta"]
    MODES = ["append", "merge"]

    def __init__(self, spark: SparkSession, df: DataFrame):
        self.spark = spark
        self.df = df

    def write(
            self, 
            fmt: str, 
            table: str,
            mode: str,
            merge_columns: list | None = None,
            partition_columns: list | None = None
        ) -> None:
        
        if fmt not in self.FORMATS:
            raise ValueError(f"Incorrect format. Use one of these options {','.join(self.FORMATS)}")
        
        if mode not in self.MODES:
            raise ValueError(f"Incorrect mode. Use one of these options {','.join(self.MODES)}")

        writer = self.df.writeTo(table).using(fmt)
        
        if partition_columns:
            writer.partitionedBy(*partition_columns)

        if not self.spark.catalog.tableExists(table):
            writer.create()
        else:
            if mode == "append":
                writer.append()
            elif mode == "merge":
                self.__write_merge(table=table, merge_columns=merge_columns)

    def __write_merge(self, table: str, merge_columns: list | None = None) -> None:
        self.df.createOrReplaceTempView("source")
        sql = f"""
            MERGE INTO {table} AS t
            USING source AS s
            ON {" AND ".join([f"t.{column}=s.{column}" for column in merge_columns])}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        self.spark.sql(sql)
        logger.info(f"Upsert Spark DataFrame records into {table} table.")
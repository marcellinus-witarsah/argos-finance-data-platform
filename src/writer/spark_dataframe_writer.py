# ================================================================================
# Author: Marcellinus A.W.
# Date: 16 May 2026
# Description: Decided to create an abstract class for extractor so that each new
# extractor has to inherite from this base class
# ================================================================================

from src.writer.base_writer import BaseWriter
from pyspark.sql import SparkSession, DataFrame
from src.utils.logger import logger


class SparkDataframeWriter(BaseWriter):

    FORMATS = ["iceberg", "delta"]
    MODES = ["append", "merge"]

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def write(
        self,
        df: DataFrame,
        fmt: str,
        table: str,
        mode: str,
        merge_columns: list | None = None,
        partition_columns: list | None = None,
    ) -> None:

        if fmt not in self.FORMATS:
            raise ValueError(
                f"Incorrect format. Use one of these options {', '.join(self.FORMATS)}"
            )

        if mode not in self.MODES:
            raise ValueError(
                f"Incorrect mode. Use one of these options {', '.join(self.MODES)}"
            )

        writer = df.writeTo(table).using(fmt)

        if partition_columns:
            logger.info(
                f"Applying partition to {table} using {', '.join(partition_columns)} ..."
            )
            writer.partitionedBy(*partition_columns)
            logger.info(
                f"Applied partition to{table} using {', '.join(partition_columns)} ..."
            )

        if not self.spark.catalog.tableExists(table):
            logger.info(f"Creating a {table} ...")
            writer.create()
            logger.info(f"Created {table}.")
        else:
            if mode == "append":
                logger.info(f"Appending Spark DataFrame records into {table} ...")
                writer.append()
                logger.info(f"Appended Spark DataFrame records into {table}.")
            elif mode == "merge":
                logger.info(f"Upserting Spark DataFrame records into {table} ...")
                self.__write_merge(df=df, table=table, merge_columns=merge_columns)
                logger.info(f"Upserted Spark DataFrame records into {table}.")

    def __write_merge(
        self, df: DataFrame, table: str, merge_columns: list | None = None
    ) -> None:
        df.createOrReplaceTempView("source")
        sql = f"""
            MERGE INTO {table} AS t
            USING source AS s
            ON {" AND ".join([f"t.{column}=s.{column}" for column in merge_columns])}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        self.spark.sql(sql)

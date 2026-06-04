# ================================================================================
# Author: Marcellinus A.W.
# Date: 16 May 2026
# Description: Decided to create an abstract class for extractor so that each new
# extractor has to inherite from this base class
# ================================================================================

from src.extractor.base_extractor import BaseExtractor
from pyspark.sql import SparkSession, DataFrame
from src.utils.logger import logger


class SparkDataframeExtractor(BaseExtractor):
    @staticmethod
    def extract(spark: SparkSession, table: str) -> DataFrame:
        try:
            logger.info(f"Extracting data via Spark table read to {table}.")
            df = spark.read.table(table)
            logger.info(f"Extracted data via via Spark table successfull.")
            return df
        except Exception as e:
            raise e

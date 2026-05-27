# ================================================================================
# Author: Marcellinus A.W.
# Date: 16 May 2026
# Description: Decided to create an abstract class for extractor so that each new
# extractor has to inherite from this base class
# ================================================================================

from src.extractor.base_extractor import BaseExtractor
from pyspark.sql import SparkSession, DataFrame
from src.utils.logger import logger

class DataframeExtractor(BaseExtractor):
    @staticmethod
    def extract(spark: SparkSession, catalog: str, schema: str, table: str) -> DataFrame:
        logger.info(f"Extracting data via Spark table read to {catalog}.{schema}.{table}.")
        df = spark.read.table(f"{catalog}.{schema}.{table}")
        logger.info(f"Extracted data via via Spark table successfull.")
        return df

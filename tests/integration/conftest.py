import unittest

from pyspark.sql import SparkSession

from src.utils.logger import logger


class Conftest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("integration_test").getOrCreate()
        logger.info("Created Spark Session.")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        logger.info("Closed Spark Session.")

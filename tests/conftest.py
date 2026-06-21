import unittest

import pytest
from pyspark.sql import SparkSession

from src.utils.logger import logger


class Conftest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("unit_test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


@pytest.fixture(scope="session")
def spark():
    logger.info("Create Spark Session for Unit Testing ...")
    spark = SparkSession.builder.master("local[1]").getOrCreate()
    yield spark
    logger.info("Stop Spark Session after Unit Testing ...")
    spark.stop()

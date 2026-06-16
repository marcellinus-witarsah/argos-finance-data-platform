import unittest
from pyspark.sql import SparkSession
from src.utils.logger import logger


class Conftest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("unit_test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
import unittest
from pyspark.sql import SparkSession

class PysparkConftest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("unit_test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
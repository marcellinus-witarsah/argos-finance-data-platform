from unittest import mock, TestCase
from src.extractor.dataframe_extractor import DataframeExtractor
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType
import json


class TestDataframeExtractor(TestCase):
    def setUp(self):
        """A function that always runs at the start of unit test."""
        self.mock_spark = mock.Mock(spec=SparkSession)
        self.mock_catalog = "argos_finance_catalog"
        self.mock_schema = "bronze"
        self.mock_table = "crypto_ohlcv"

    def tearDown(self):
        """A function that always runs after unit test function is run."""
        pass

    def test_extract_function_reads_correct_table(self):
        DataframeExtractor.extract(
            self.mock_spark, self.mock_catalog, self.mock_schema, self.mock_table
        )
        self.mock_spark.read.table.assert_called_once_with(
            "argos_finance_catalog.bronze.crypto_ohlcv"
        )

    def test_function_return_spark_dataframe_type_on_success(self):
        mock_spark_dataframe = mock.Mock(spec=DataFrame)
        self.mock_spark.read.table.return_value = mock_spark_dataframe
        df = DataframeExtractor.extract(
            self.mock_spark, self.mock_catalog, self.mock_schema, self.mock_table
        )
        self.assertIsInstance(df, DataFrame)

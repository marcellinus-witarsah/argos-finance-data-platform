from unittest import mock, TestCase
from src.extractor.spark_dataframe_extractor import SparkDataframeExtractor
from pyspark.sql import DataFrame, SparkSession


class TestDataframeExtractor(TestCase):
    def setUp(self):
        """A function that always runs at the start of unit test."""
        self.mock_spark = mock.Mock(spec=SparkSession)
        self.mock_table = "argos_finance_catalog.bronze.crypto_ohlcv"

    def tearDown(self):
        """A function that always runs after unit test function is run."""
        pass

    def test_extract_function_read_correct_table(self):
        SparkDataframeExtractor.extract(self.mock_spark, self.mock_table)
        self.mock_spark.read.table.assert_called_once_with(
            "argos_finance_catalog.bronze.crypto_ohlcv"
        )

    def test_extract_function_return_spark_dataframe_type_on_success(self):
        mock_spark_dataframe = mock.Mock(spec=DataFrame)
        self.mock_spark.read.table.return_value = mock_spark_dataframe
        df = SparkDataframeExtractor.extract(self.mock_spark, self.mock_table)
        self.assertIsInstance(df, DataFrame)

    def test_extract_function_raise_exception_on_failed(self):
        self.mock_spark.read.table.side_effect = Exception()
        with self.assertRaises(Exception):
            SparkDataframeExtractor.extract(self.mock_spark, "invalid")

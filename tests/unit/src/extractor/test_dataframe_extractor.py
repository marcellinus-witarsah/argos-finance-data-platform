import chispa
import pytest
from pyspark.sql import DataFrame, DataFrameReader
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from src.extractor.spark_dataframe_extractor import SparkDataframeExtractor


class TestDataframeExtractor:
    # def setUp(self, spark):
    #     """A function that always runs at the start of unit test."""
    #     self.mock_spark = mock.Mock(spec=SparkSession)
    #     self.mock_table = "argos_finance_catalog.bronze.crypto_ohlcv"

    # def tearDown(self):
    #     """A function that always runs after unit test function is run."""
    #     pass

    def test_extract_function_read_correct_table_on_success(self, spark):
        mock_table = "mock_table"
        expected_df = spark.createDataFrame(
            data=[("BTC", 65_000.00), ("ETH", 2_000.00)],
            schema=StructType(
                [
                    StructField("symbol", StringType(), nullable=True),
                    StructField("price", DoubleType(), nullable=True),
                ]
            ),
        )
        expected_df.createOrReplaceTempView(mock_table)
        spark_data_frame_extractor = SparkDataframeExtractor(spark=spark)
        df = spark_data_frame_extractor.extract(mock_table)
        chispa.assert_df_equality(expected_df, df)

    def test_extract_function_return_spark_dataframe_type_on_success(self, spark):
        mock_table = "mock_table"
        expected_df = spark.createDataFrame([()])
        expected_df.createOrReplaceTempView(mock_table)
        spark_data_frame_extractor = SparkDataframeExtractor(spark=spark)
        df = spark_data_frame_extractor.extract(mock_table)
        assert isinstance(df, DataFrame)

    def test_extract_function_raise_exception_on_failed(self, spark, mocker):
        mocker.patch.object(
            DataFrameReader, "table", side_effect=Exception("Table not found")
        )
        spark_data_frame_extractor = SparkDataframeExtractor(spark=spark)
        with pytest.raises(Exception, match="Table not found"):
            spark_data_frame_extractor.extract("mock_table")

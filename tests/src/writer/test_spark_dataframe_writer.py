import unittest
from unittest import mock
from src.writer.spark_dataframe_writer import SparkDataframeWriter
from pyspark.sql import SparkSession, DataFrame, DataFrameWriterV2


class TestSparkDataframeWriter(unittest.TestCase):
    def setUp(self):
        self.mock_spark = mock.Mock(spec=SparkSession)
        self.mock_df = mock.Mock(spec=DataFrame)

        self.mock_df_writer_v2 = mock.Mock(spec=DataFrameWriterV2)
        self.mock_df_writer_v2.using.return_value = self.mock_df_writer_v2
        self.mock_df_writer_v2.option.return_value = self.mock_df_writer_v2
        self.mock_df_writer_v2.partitionedBy.return_value = self.mock_df_writer_v2
        self.mock_df_writer_v2.append.return_value = None
        self.mock_df_writer_v2.create.return_value = None
        self.mock_df.writeTo.return_value = self.mock_df_writer_v2
        self.mock_df.createOrReplaceTempView.return_value = None

        self.spark_dataframe_writer = SparkDataframeWriter(self.mock_spark)

    def test_raises_value_error_using_incorrect_fmt_argument(self):
        """Test if it raised an error due to incorrect `fmt` argument"""
        with self.assertRaises(ValueError):
            self.spark_dataframe_writer.write(
                df=self.mock_df,
                fmt="incorrect",
                table="argos_finance_catalog.silver.crypto_ohlcv",
                mode="create",
            )

    def test_raises_value_error_using_incorrect_mode_argument(self):
        """Test if it raised an error due to inccorrect `mode` argument"""
        with self.assertRaises(ValueError):
            self.spark_dataframe_writer.write(
                df=self.mock_df,
                fmt="iceberg",
                table="argos_finance_catalog.silver.crypto_ohlcv",
                mode="incorect",
            )

    # Create a partition table
    def test_write_function_calls_partitionedBy_function(self):
        """Test"""
        self.mock_spark.catalog.tableExists.return_value = False
        self.spark_dataframe_writer.write(
            df=self.mock_df,
            fmt="iceberg",
            table="argos_finance_catalog.silver.crypto_ohlcv",
            mode="append",
            partition_columns=["partition_column_1", "partition_column_2"],
        )
        self.mock_df_writer_v2.partitionedBy.assert_called_once()

    def test_write_function_calls_partitionedBy_function_with_correct_col_arguments(
        self,
    ):
        self.mock_spark.catalog.tableExists.return_value = False
        self.spark_dataframe_writer.write(
            df=self.mock_df,
            fmt="iceberg",
            table="argos_finance_catalog.silver.crypto_ohlcv",
            mode="append",
            partition_columns=["partition_column_1", "partition_column_2"],
        )
        self.mock_df_writer_v2.partitionedBy.assert_called_once_with(
            "partition_column_1", "partition_column_2"
        )

    # Create Table
    def test_write_function_calls_create_function(self):
        self.mock_spark.catalog.tableExists.return_value = False
        self.spark_dataframe_writer.write(
            df=self.mock_df,
            fmt="iceberg",
            table="argos_finance_catalog.silver.crypto_ohlcv",
            mode="append",
        )
        self.mock_df_writer_v2.create.assert_called_once()

    def test_writer_function_with_correct_format(self):
        self.mock_spark.catalog.tableExists.return_value = False
        self.spark_dataframe_writer.write(
            df=self.mock_df,
            fmt="iceberg",
            table="argos_finance_catalog.silver.crypto_ohlcv",
            mode="append",
        )
        self.mock_df_writer_v2.create.assert_called_once()

    # Append Function
    def test_write_function_calls_append_function(self):
        self.mock_spark.catalog.tableExists.return_value = True
        self.spark_dataframe_writer.write(
            df=self.mock_df,
            fmt="iceberg",
            table="argos_finance_catalog.silver.crypto_ohlcv",
            mode="append",
        )
        self.mock_df_writer_v2.append.assert_called_once()

    def test_append_function_called_with_correct_table_argument(self):
        self.mock_spark.catalog.tableExists.return_value = True
        self.spark_dataframe_writer.write(
            df=self.mock_df,
            fmt="iceberg",
            table="argos_finance_catalog.silver.crypto_ohlcv",
            mode="append",
        )
        self.mock_df.writeTo.assert_called_once_with(
            "argos_finance_catalog.silver.crypto_ohlcv"
        )

    def test_append_function_called_with_correct_fmt_argument(self):
        self.mock_spark.catalog.tableExists.return_value = True
        self.spark_dataframe_writer.write(
            df=self.mock_df,
            fmt="iceberg",
            table="argos_finance_catalog.silver.crypto_ohlcv",
            mode="append",
        )
        self.mock_df_writer_v2.using.assert_called_once_with("iceberg")

    # Test __write_merge behavior
    def test_write_function_calls_write_merge_function(self):
        "To identify if __write_merge() function is called, we can identify if the spark sql function is called through self.mock_spark that we created"
        self.mock_spark.catalog.tableExists.return_value = True
        self.spark_dataframe_writer.write(
            df=self.mock_df,
            fmt="iceberg",
            table="argos_finance_catalog.silver.crypto_ohlcv",
            mode="merge",
            merge_columns=["merge_column_1", "merge_column_2"],
        )
        self.mock_spark.sql.assert_called_once()

    def test_merge_function_calls_createOrReplaceTempView_function(self):
        self.mock_spark.catalog.tableExists.return_value = True
        self.spark_dataframe_writer.write(
            df=self.mock_df,
            fmt="iceberg",
            table="argos_finance_catalog.silver.crypto_ohlcv",
            mode="merge",
            merge_columns=["merge_column_1", "merge_column_2"],
        )
        self.mock_df.createOrReplaceTempView.assert_called_once()

    def test_merge_function_constructs_correct_sql_script(self):
        self.mock_spark.catalog.tableExists.return_value = True
        self.spark_dataframe_writer.write(
            df=self.mock_df,
            fmt="iceberg",
            table="argos_finance_catalog.silver.crypto_ohlcv",
            mode="merge",
            merge_columns=["merge_column_1", "merge_column_2"],
        )
        sql_script = self.mock_spark.sql.call_args[0][0]
        self.assertIn(
            "MERGE INTO argos_finance_catalog.silver.crypto_ohlcv AS t", sql_script
        )
        self.assertIn("USING source AS s", sql_script)
        self.assertIn(
            "ON t.merge_column_1=s.merge_column_1 AND t.merge_column_2=s.merge_column_2",
            sql_script,
        )
        self.assertIn("WHEN MATCHED THEN UPDATE SET *", sql_script)
        self.assertIn("WHEN NOT MATCHED THEN INSERT *", sql_script)

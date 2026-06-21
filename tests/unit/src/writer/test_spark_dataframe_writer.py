
import pytest
from pyspark.sql import DataFrame, DataFrameWriterV2, SparkSession

from src.writer.spark_dataframe_writer import SparkDataframeWriter


class TestSparkDataframeWriter:
    # def setUp(self):
    #     self.mock_spark = mock.Mock(spec=SparkSession)
    #     self.mock_df = mock.Mock(spec=DataFrame)

    #     self.mock_df_writer_v2 = mock.Mock(spec=DataFrameWriterV2)
    #     self.mock_df_writer_v2.using.return_value = self.mock_df_writer_v2
    #     self.mock_df_writer_v2.option.return_value = self.mock_df_writer_v2
    #     self.mock_df_writer_v2.partitionedBy.return_value = self.mock_df_writer_v2
    #     self.mock_df_writer_v2.append.return_value = None
    #     self.mock_df_writer_v2.create.return_value = None
    #     self.mock_df.writeTo.return_value = self.mock_df_writer_v2
    #     self.mock_df.createOrReplaceTempView.return_value = None

    def test_raises_value_error_using_incorrect_fmt_argument(self, mocker):
        """Test if it raised an error due to incorrect `fmt` argument"""
        mock_spark = mocker.Mock()
        mock_df = mocker.Mock()
        # mock_df_writer_v2 = mocker.Mock(spec=DataFrameWriterV2)
        # mock_df_writer_v2.using.return_value = mock_df_writer_v2
        # mock_df_writer_v2.option.return_value = mock_df_writer_v2
        # mock_df_writer_v2.partitionedBy.return_value = mock_df_writer_v2
        # mock_df_writer_v2.append.return_value = None
        # mock_df_writer_v2.create.return_value = None
        # mock_df.writeTo.return_value = mock_df_writer_v2
        # mock_df.createOrReplaceTempView.return_value = None
        spark_data_frame_writer = SparkDataframeWriter(spark=mock_spark)
        with pytest.raises(ValueError):
            spark_data_frame_writer.write(
                df=mock_df,
                fmt="incorrect",
                table="argos_finance_catalog.silver.crypto_ohlcv",
                mode="create",
            )

    def test_raises_value_error_using_incorrect_mode_argument(self, mocker):
        """Test if it raised an error due to inccorrect `mode` argument"""
        mock_spark = mocker.Mock()
        mock_df = mocker.Mock()
        spark_data_frame_writer = SparkDataframeWriter(spark=mock_spark)
        with pytest.raises(ValueError):
            spark_data_frame_writer.write(
                df=mock_df,
                fmt="iceberg",
                table="argos_finance_catalog.silver.crypto_ohlcv",
                mode="incorect",
            )

    # Create a partition table
    def test_write_function_calls_partitionedBy_function(self, mocker):
        """Test"""
        mock_spark = mocker.Mock(spec=SparkSession)
        mock_spark.catalog.tableExists.return_value = False

        mock_df = mocker.Mock(spec=DataFrame)
        mock_df_writer_v2 = mocker.Mock(spec=DataFrameWriterV2)
        mock_df_writer_v2.using.return_value = mock_df_writer_v2
        mock_df_writer_v2.partitionedBy.return_value = mock_df_writer_v2
        mock_df_writer_v2.create.return_value = None
        mock_df.writeTo.return_value = mock_df_writer_v2

        spark_data_frame_writer = SparkDataframeWriter(spark=mock_spark)
        spark_data_frame_writer.write(
            df=mock_df,
            fmt="iceberg",
            table="argos_finance_catalog.silver.crypto_ohlcv",
            mode="append",
            partition_columns=["partition_column_1", "partition_column_2"],
        )

        mock_df_writer_v2.partitionedBy.assert_called_once()

    def test_write_function_calls_partitionedBy_function_with_correct_col_arguments(
        self, mocker
    ):
        mock_spark = mocker.Mock(spec=SparkSession)
        mock_spark.catalog.tableExists.return_value = False

        mock_df = mocker.Mock(spec=DataFrame)
        mock_df_writer_v2 = mocker.Mock(spec=DataFrameWriterV2)
        mock_df_writer_v2.using.return_value = mock_df_writer_v2
        mock_df_writer_v2.partitionedBy.return_value = mock_df_writer_v2
        mock_df_writer_v2.create.return_value = None
        mock_df.writeTo.return_value = mock_df_writer_v2

        spark_data_frame_writer = SparkDataframeWriter(spark=mock_spark)
        spark_data_frame_writer.write(
            df=mock_df,
            fmt="iceberg",
            table="argos_finance_catalog.silver.crypto_ohlcv",
            mode="append",
            partition_columns=["partition_column_1", "partition_column_2"],
        )

        mock_df_writer_v2.partitionedBy.assert_called_once_with(
            "partition_column_1", "partition_column_2"
        )

    # Create Table
    def test_write_function_calls_create_function(self, mocker):
        mock_spark = mocker.Mock(spec=SparkSession)
        mock_spark.catalog.tableExists.return_value = False

        mock_df = mocker.Mock(spec=DataFrame)
        mock_df_writer_v2 = mocker.Mock(spec=DataFrameWriterV2)
        mock_df_writer_v2.using.return_value = mock_df_writer_v2
        mock_df_writer_v2.partitionedBy.return_value = mock_df_writer_v2
        mock_df_writer_v2.create.return_value = None
        mock_df.writeTo.return_value = mock_df_writer_v2

        spark_data_frame_writer = SparkDataframeWriter(spark=mock_spark)
        spark_data_frame_writer.write(
            df=mock_df,
            fmt="iceberg",
            table="argos_finance_catalog.silver.crypto_ohlcv",
            mode="append",
            partition_columns=["partition_column_1", "partition_column_2"],
        )
        mock_df_writer_v2.create.assert_called_once()

    # Append Function
    def test_write_function_calls_append_function(self, mocker):
        mock_spark = mocker.Mock(spec=SparkSession)
        mock_spark.catalog.tableExists.return_value = True

        mock_df = mocker.Mock(spec=DataFrame)
        mock_df_writer_v2 = mocker.Mock(spec=DataFrameWriterV2)
        mock_df_writer_v2.using.return_value = mock_df_writer_v2
        mock_df_writer_v2.partitionedBy.return_value = mock_df_writer_v2
        mock_df_writer_v2.create.return_value = None
        mock_df.writeTo.return_value = mock_df_writer_v2

        spark_data_frame_writer = SparkDataframeWriter(spark=mock_spark)
        spark_data_frame_writer.write(
            df=mock_df,
            fmt="iceberg",
            table="argos_finance_catalog.silver.crypto_ohlcv",
            mode="append",
            partition_columns=["partition_column_1", "partition_column_2"],
        )
        mock_df_writer_v2.append.assert_called_once()

    def test_append_function_called_with_correct_fmt_argument(self, mocker):
        mock_spark = mocker.Mock(spec=SparkSession)
        mock_spark.catalog.tableExists.return_value = True

        mock_df = mocker.Mock(spec=DataFrame)
        mock_df_writer_v2 = mocker.Mock(spec=DataFrameWriterV2)
        mock_df_writer_v2.using.return_value = mock_df_writer_v2
        mock_df_writer_v2.partitionedBy.return_value = mock_df_writer_v2
        mock_df_writer_v2.create.return_value = None
        mock_df.writeTo.return_value = mock_df_writer_v2

        spark_data_frame_writer = SparkDataframeWriter(spark=mock_spark)
        spark_data_frame_writer.write(
            df=mock_df,
            fmt="iceberg",
            table="argos_finance_catalog.silver.crypto_ohlcv",
            mode="append",
            partition_columns=["partition_column_1", "partition_column_2"],
        )
        mock_df_writer_v2.using.assert_called_once_with("iceberg")

    # Test __write_merge behavior
    def test_write_function_calls_write_merge_function(self, mocker):
        "To identify if __write_merge() function is called, we can identify if the spark sql function is called through self.mock_spark that we created"
        mock_spark = mocker.Mock(spec=SparkSession)
        mock_spark.catalog.tableExists.return_value = True

        mock_df = mocker.Mock(spec=DataFrame)
        mock_df_writer_v2 = mocker.Mock(spec=DataFrameWriterV2)
        mock_df_writer_v2.using.return_value = mock_df_writer_v2
        mock_df_writer_v2.partitionedBy.return_value = mock_df_writer_v2
        mock_df_writer_v2.create.return_value = None
        mock_df.writeTo.return_value = mock_df_writer_v2

        spark_data_frame_writer = SparkDataframeWriter(spark=mock_spark)
        spy = mocker.spy(spark_data_frame_writer, "_SparkDataframeWriter__write_merge")
        spark_data_frame_writer.write(
            df=mock_df,
            fmt="iceberg",
            table="argos_finance_catalog.silver.crypto_ohlcv",
            mode="merge",
            partition_columns=["partition_column_1", "partition_column_2"],
            merge_columns=["merge_column_1", "merge_column_2"],
        )
        spy.assert_called_once()

    def test_write_function_calls_write_merge_function_with_correct_arguments(
        self, mocker
    ):
        "To identify if __write_merge() function is called, we can identify if the spark sql function is called through self.mock_spark that we created"
        mock_spark = mocker.Mock(spec=SparkSession)
        mock_spark.catalog.tableExists.return_value = True

        mock_df = mocker.Mock(spec=DataFrame)
        mock_df_writer_v2 = mocker.Mock(spec=DataFrameWriterV2)
        mock_df_writer_v2.using.return_value = mock_df_writer_v2
        mock_df_writer_v2.partitionedBy.return_value = mock_df_writer_v2
        mock_df_writer_v2.create.return_value = None
        mock_df.writeTo.return_value = mock_df_writer_v2

        spark_data_frame_writer = SparkDataframeWriter(spark=mock_spark)
        spy = mocker.spy(spark_data_frame_writer, "_SparkDataframeWriter__write_merge")
        spark_data_frame_writer.write(
            df=mock_df,
            fmt="iceberg",
            table="argos_finance_catalog.silver.crypto_ohlcv",
            mode="merge",
            partition_columns=["partition_column_1", "partition_column_2"],
            merge_columns=["merge_column_1", "merge_column_2"],
        )
        spy.assert_called_once_with(
            mock_df,
            "argos_finance_catalog.silver.crypto_ohlcv",
            ["merge_column_1", "merge_column_2"],
        )

    def test_merge_function_calls_createOrReplaceTempView_function(self, mocker):
        mock_spark = mocker.Mock(spec=SparkSession)
        mock_spark.catalog.tableExists.return_value = True

        mock_df = mocker.Mock(spec=DataFrame)
        mock_df_writer_v2 = mocker.Mock(spec=DataFrameWriterV2)
        mock_df_writer_v2.using.return_value = mock_df_writer_v2
        mock_df_writer_v2.partitionedBy.return_value = mock_df_writer_v2
        mock_df_writer_v2.create.return_value = None
        mock_df.writeTo.return_value = mock_df_writer_v2

        spark_data_frame_writer = SparkDataframeWriter(spark=mock_spark)
        spark_data_frame_writer.write(
            df=mock_df,
            fmt="iceberg",
            table="argos_finance_catalog.silver.crypto_ohlcv",
            mode="merge",
            partition_columns=["partition_column_1", "partition_column_2"],
            merge_columns=["merge_column_1", "merge_column_2"],
        )
        mock_df.createOrReplaceTempView.assert_called_once()

    def test_merge_function_constructs_correct_sql_script(self, mocker):
        mock_spark = mocker.Mock(spec=SparkSession)
        mock_spark.catalog.tableExists.return_value = True

        mock_df = mocker.Mock(spec=DataFrame)
        mock_df_writer_v2 = mocker.Mock(spec=DataFrameWriterV2)
        mock_df_writer_v2.using.return_value = mock_df_writer_v2
        mock_df_writer_v2.partitionedBy.return_value = mock_df_writer_v2
        mock_df_writer_v2.create.return_value = None
        mock_df.writeTo.return_value = mock_df_writer_v2

        spark_data_frame_writer = SparkDataframeWriter(spark=mock_spark)
        spark_data_frame_writer.write(
            df=mock_df,
            fmt="iceberg",
            table="argos_finance_catalog.silver.crypto_ohlcv",
            mode="merge",
            partition_columns=["partition_column_1", "partition_column_2"],
            merge_columns=["merge_column_1", "merge_column_2"],
        )
        sql_script = mock_spark.sql.call_args[0][0]
        assert "MERGE INTO argos_finance_catalog.silver.crypto_ohlcv AS t" in sql_script
        assert "USING source AS s" in sql_script
        assert (
            "ON t.merge_column_1=s.merge_column_1 AND t.merge_column_2=s.merge_column_2"
            in sql_script
        )
        assert "WHEN MATCHED THEN UPDATE SET *" in sql_script
        assert "WHEN NOT MATCHED THEN INSERT *" in sql_script

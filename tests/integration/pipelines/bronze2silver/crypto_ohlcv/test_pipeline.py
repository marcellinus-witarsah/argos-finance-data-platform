import datetime
import json

import chispa
import pyspark.sql.functions as F
from pyspark.sql.types import (DateType, DoubleType, StringType, StructField,
                               StructType, TimestampType)

from pipelines.bronze2silver.crypto_ohclv.pipeline import main

json_data = {
    "Meta Data": {
        "1. Information": "Daily Prices and Volumes for Digital Currency",
        "2. Digital Currency Code": "BTC",
        "3. Digital Currency Name": "Bitcoin",
        "4. Market Code": "USD",
        "5. Market Name": "USD",
        "6. Last Refreshed": "2024-01-02 00:00:00",
        "7. Time Zone": "UTC",
    },
    "Time Series (Digital Currency Daily)": {
        "2024-01-02": {
            "1. open": "42000.00",
            "2. high": "43500.00",
            "3. low": "41800.00",
            "4. close": "43200.00",
            "5. volume": "15234.56",
        },
        "2024-01-01": {
            "1. open": "40000.00",
            "2. high": "40000.00",
            "3. low": "40000.00",
            "4. close": "40000.00",
            "5. volume": "23234.56",
        },
    },
}


class TestCryptoOHLCVPipeline:

    def test_run_writes_to_spark_table(self, spark, mocker):
        source_table_name = "argos_finance_catalog.bronze.alpha_vantage_crypto_ohlcv"

        FIXED_TIMESTAMP = datetime.datetime(2024, 1, 2, 7, 0, 0)
        FIXED_DATE = datetime.date(2024, 1, 2)
        source_df = spark.createDataFrame(
            data=[
                (json.dumps(json_data), "1", FIXED_TIMESTAMP, FIXED_DATE),
            ],
            schema=StructType(
                [
                    StructField("json_data", StringType(), True),
                    StructField("id", StringType(), True),
                    StructField("load_dttm", TimestampType(), True),
                    StructField("load_prdt", DateType(), True),
                ]
            ),
        )
        source_df.writeTo(source_table_name).using("iceberg").create()

        mocker.patch("src.utils.spark_session.get_spark", new=spark)

        mocker.patch(
            "pipelines.shared.transform.F.current_timestamp",
            return_value=F.lit(FIXED_TIMESTAMP),
        )
        mocker.patch(
            "pipelines.shared.transform.F.to_date", return_value=F.lit(FIXED_DATE)
        )

        expected_df = spark.createDataFrame(
            data=[
                (
                    "BTC",
                    "USD",
                    datetime.date(2024, 1, 2),
                    42000.00,
                    43500.00,
                    41800.00,
                    43200.00,
                    15234.56,
                    FIXED_TIMESTAMP,
                    FIXED_DATE,
                ),
                (
                    "BTC",
                    "USD",
                    datetime.date(2024, 1, 1),
                    40000.00,
                    40000.00,
                    40000.00,
                    40000.00,
                    23234.56,
                    FIXED_TIMESTAMP,
                    FIXED_DATE,
                ),
            ],
            schema=StructType(
                [
                    StructField("ticker", StringType(), True),
                    StructField("currency", StringType(), True),
                    StructField("date", DateType(), True),
                    StructField("open", DoubleType(), True),
                    StructField("high", DoubleType(), True),
                    StructField("low", DoubleType(), True),
                    StructField("close", DoubleType(), True),
                    StructField("volume", DoubleType(), True),
                    StructField("load_dttm", TimestampType(), True),
                    StructField("load_prdt", DateType(), True),
                ]
            ),
        )
        table_name = "argos_finance_catalog.silver.crypto_ohlcv"
        main()
        df = spark.read.table(table_name)

        assert df.count() == 2
        chispa.dataframe_comparer.assert_schema_equality(
            expected_df.schema, df.schema, ignore_nullable=True
        )

        chispa.dataframe_comparer.assert_df_equality(
            expected_df, df, ignore_nullable=True, ignore_row_order=True
        )

        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.sql(f"DROP TABLE IF EXISTS {source_table_name}")

import datetime

import chispa
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
import pyspark.sql.functions as F
from pipelines.silver2gold.bitcoin_ohlcv_vs_fed_interest_rates.pipeline import run

SAMPLE_CFG = {
    "extractor": {},
    "transformer": {},
    "writer": {
        "table": " argos_finance_catalog.gold.bitcoin_ohlcv_vs_fed_interest_rates",
        "fmt": "iceberg",
        "mode": "merge",
        "merge_columns": ["ticker", "date"],
    },
}


class TestBitcoinOHLCVVSFedInterestRatesPipeline:
    def test_run_writes_to_spark_table(self, spark, mocker):
        FIXED_TIMESTAMP = datetime.datetime(2024, 1, 2, 7, 0, 0)
        FIXED_DATE = datetime.date(2024, 1, 2)

        

        silver_crypto_ohlcv_df = spark.createDataFrame(
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

        silver_interest_rates_df = spark.createDataFrame(
            data=[
                (datetime.date(2024, 1, 1), 4.33, FIXED_TIMESTAMP, FIXED_DATE),
            ],
            schema=StructType(
                [
                    StructField("date", DateType(), True),
                    StructField("rate", DoubleType(), True),
                    StructField("load_dttm", TimestampType(), True),
                    StructField("load_prdt", DateType(), True),
                ]
            ),
        )

        # Write to spark table
        spark.sql(f"DROP TABLE IF EXISTS argos_finance_catalog.silver.crypto_ohlcv")
        silver_crypto_ohlcv_df.writeTo(
            "argos_finance_catalog.silver.crypto_ohlcv"
        ).using("iceberg").create()

        spark.sql(f"DROP TABLE IF EXISTS argos_finance_catalog.silver.interest_rates")
        silver_interest_rates_df.writeTo(
            "argos_finance_catalog.silver.interest_rates"
        ).using("iceberg").create()

        mocker.patch(
            "pipelines.shared.transform.F.current_timestamp",
            return_value=F.lit(FIXED_TIMESTAMP),
        )
        mocker.patch(
            "pipelines.shared.transform.F.to_date", return_value=F.lit(FIXED_DATE)
        )

        mocker.patch("src.utils.spark_session.get_spark", new=spark)

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
                    4.33,
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
                    4.33,
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
                    StructField("rate", DoubleType(), True),
                    StructField("load_dttm", TimestampType(), True),
                    StructField("load_prdt", DateType(), True),
                ]
            ),
        )

        run(cfg=SAMPLE_CFG)

        df = spark.read.table("argos_finance_catalog.gold.bitcoin_ohlcv_vs_fed_interest_rates")

        assert expected_df.count() == df.count()

        chispa.schema_comparer.assert_schema_equality(
            expected_df.schema, df.schema, ignore_nullable=True
        )

        chispa.assert_df_equality(
            expected_df, df, ignore_nullable=True, ignore_row_order=True
        )

import datetime

import chispa
import pyspark.sql.functions as F
from pyspark.sql.types import (DateType, DoubleType, IntegerType, MapType,
                               StringType, StructField, StructType)

from pipelines.bronze2silver.crypto_ohclv.transform import (explode_json,
                                                            parse_json,
                                                            select_column)


class TestTransform:
    def test_parse_json(self, spark):
        df = spark.createDataFrame(
            [('{"symbol": "BTC","price": 60000}', "BTC", 60000)],
            schema=StructType(
                [
                    StructField("json_data", StringType(), True),
                    StructField("symbol", StringType(), True),
                    StructField("price", IntegerType(), True),
                ]
            ),
        )

        df = df.withColumn(
            "expected_parsed_json",
            F.struct(
                F.col("symbol"),
                F.col("price"),
            ),
        )

        df = df.transform(
            parse_json,
            col="json_data",
            schema=StructType(
                [
                    StructField("symbol", StringType(), True),
                    StructField("price", IntegerType(), True),
                ]
            ),
        )

        chispa.assert_column_equality(df, "parsed_json", "expected_parsed_json")

    def test_explode_json(self, spark):
        schema = StructType(
            [
                StructField(
                    "Meta Data",
                    StructType(
                        [
                            StructField("2. Digital Currency Code", StringType()),
                            StructField("4. Market Code", StringType()),
                        ]
                    ),
                ),
                StructField(
                    "Time Series (Digital Currency Daily)",
                    MapType(
                        StringType(),
                        StructType(
                            [
                                StructField("1. open", StringType()),
                                StructField("2. high", StringType()),
                                StructField("3. low", StringType()),
                                StructField("4. close", StringType()),
                                StructField("5. volume", StringType()),
                            ]
                        ),
                    ),
                ),
            ]
        )

        json_data = """
        {
            "Meta Data": {
                "2. Digital Currency Code": "BTC",
                "4. Market Code": "EUR"
            },
            "Time Series (Digital Currency Daily)": {
                "2026-06-10": {
                    "1. open": "53505.38000000",
                    "2. high": "53614.71000000",
                    "3. low": "53405.63000000",
                    "4. close": "53408.24000000",
                    "5. volume": "6.65581542"
                },
                "2026-06-09": {
                    "1. open": "54714.98000000",
                    "2. high": "55003.51000000",
                    "3. low": "52660.00000000",
                    "4. close": "53509.52000000",
                    "5. volume": "604.80028130"
                }
            }
        }
        """

        df = spark.createDataFrame(
            [(json_data,)],
            schema=StructType(
                [
                    StructField("json_data", StringType(), True),
                ]
            ),
        )

        df = df.withColumn(
            "parsed_json", F.from_json(F.col("json_data"), schema=schema)
        )

        expected_df = spark.createDataFrame(
            [
                (
                    "BTC",
                    "EUR",
                    "2026-06-10",
                    """
                        {
                            "1. open": "53505.38000000",
                            "2. high": "53614.71000000",
                            "3. low": "53405.63000000",
                            "4. close": "53408.24000000",
                            "5. volume": "6.65581542"
                        },
                    """,
                ),
                (
                    "BTC",
                    "EUR",
                    "2026-06-09",
                    """
                        {
                            "1. open": "54714.98000000",
                            "2. high": "55003.51000000",
                            "3. low": "52660.00000000",
                            "4. close": "53509.52000000",
                            "5. volume": "604.80028130"
                        }
                    """,
                ),
            ],
            schema=StructType(
                [
                    StructField("ticker", StringType(), True),
                    StructField("currency", StringType(), True),
                    StructField("date", StringType(), True),
                    StructField("ohlcv", StringType(), True),
                ]
            ),
        )

        expected_df = expected_df.withColumn(
            "ohlcv",
            F.from_json(
                F.col("ohlcv"),
                schema=StructType(
                    [
                        StructField("1. open", StringType()),
                        StructField("2. high", StringType()),
                        StructField("3. low", StringType()),
                        StructField("4. close", StringType()),
                        StructField("5. volume", StringType()),
                    ]
                ),
            ),
        )

        df = df.transform(explode_json, col="parsed_json")

        chispa.assert_df_equality(df, expected_df, ignore_nullable=True)

    def test_select_column(self, spark):
        df = spark.createDataFrame(
            [
                (
                    "BTC",
                    "EUR",
                    "2026-06-10",
                    """
                        {
                            "1. open": "53505.38000000",
                            "2. high": "53614.71000000",
                            "3. low": "53405.63000000",
                            "4. close": "53408.24000000",
                            "5. volume": "6.65581542"
                        },
                    """,
                ),
                (
                    "BTC",
                    "EUR",
                    "2026-06-09",
                    """
                        {
                            "1. open": "54714.98000000",
                            "2. high": "55003.51000000",
                            "3. low": "52660.00000000",
                            "4. close": "53509.52000000",
                            "5. volume": "604.80028130"
                        }
                    """,
                ),
            ],
            schema=StructType(
                [
                    StructField("ticker", StringType(), True),
                    StructField("currency", StringType(), True),
                    StructField("date", StringType(), True),
                    StructField("ohlcv", StringType(), True),
                ]
            ),
        )

        df = df.withColumn(
            "ohlcv",
            F.from_json(
                F.col("ohlcv"),
                schema=StructType(
                    [
                        StructField("1. open", StringType()),
                        StructField("2. high", StringType()),
                        StructField("3. low", StringType()),
                        StructField("4. close", StringType()),
                        StructField("5. volume", StringType()),
                    ]
                ),
            ),
        )

        expected_df = spark.createDataFrame(
            [
                (
                    "BTC",
                    "EUR",
                    datetime.date(2026, 6, 10),
                    53505.38000000,
                    53614.71000000,
                    53405.63000000,
                    53408.24000000,
                    6.65581542,
                ),
                (
                    "BTC",
                    "EUR",
                    datetime.date(2026, 6, 9),
                    54714.98000000,
                    55003.51000000,
                    52660.00000000,
                    53509.52000000,
                    604.80028130,
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
                ]
            ),
        )

        df = df.transform(select_column)

        chispa.assert_df_equality(df, expected_df, ignore_nullable=True)

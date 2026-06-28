import datetime

import chispa
import pyspark.sql.functions as F
from pyspark.sql.types import (ArrayType, DateType, DoubleType, StringType,
                               StructField, StructType)

from pipelines.bronze2silver.interest_rates.transform import (explode_json,
                                                              select_column)


class TestTransform:
    def test_explode_json(self, spark):
        schema = StructType(
            [
                StructField(
                    "observations",
                    ArrayType(
                        StructType(
                            [
                                StructField("realtime_start", StringType(), True),
                                StructField("realtime_end", StringType(), True),
                                StructField("date", StringType(), True),
                                StructField("value", StringType(), True),
                            ]
                        )
                    ),
                    True,
                ),
            ]
        )

        df = spark.createDataFrame(
            [
                (
                    """
                        {
                            "observations": [
                                {
                                    "realtime_start": "2013-08-14",
                                    "realtime_end": "2013-08-14",
                                    "date": "1929-01-01",
                                    "value": "1065.9"
                                },
                                {
                                    "realtime_start": "2013-08-14",
                                    "realtime_end": "2013-08-14",
                                    "date": "1930-01-01",
                                    "value": "975.5"
                                }
                            ]
                        }
                    """,
                )
            ],
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
                    """
                        {
                            "realtime_start": "2013-08-14",
                            "realtime_end": "2013-08-14",
                            "date": "1929-01-01",
                            "value": "1065.9"
                        }
                    """,
                ),
                (
                    """
                        {
                            "realtime_start": "2013-08-14",
                            "realtime_end": "2013-08-14",
                            "date": "1930-01-01",
                            "value": "975.5"
                        }
                    """,
                ),
            ],
            schema=StructType(
                [
                    StructField("json_data", StringType(), True),
                ]
            ),
        )
        expected_df = expected_df.withColumn(
            "observations",
            F.from_json(
                F.col("json_data"),
                schema=StructType(
                    [
                        StructField("realtime_start", StringType(), True),
                        StructField("realtime_end", StringType(), True),
                        StructField("date", StringType(), True),
                        StructField("value", StringType(), True),
                    ]
                ),
            ),
        ).drop("json_data")

        df = df.transform(explode_json, col="parsed_json")

        chispa.assert_df_equality(df, expected_df, ignore_nullable=True)

    def test_select_column(self, spark):
        df = spark.createDataFrame(
            [
                (
                    """
                        {
                            "realtime_start": "2013-08-14",
                            "realtime_end": "2013-08-14",
                            "date": "1929-01-01",
                            "value": "1065.9"
                        }
                    """,
                ),
                (
                    """
                        {
                            "realtime_start": "2013-08-14",
                            "realtime_end": "2013-08-14",
                            "date": "1930-01-01",
                            "value": "975.5"
                        }
                    """,
                ),
            ],
            schema=StructType(
                [
                    StructField("json_data", StringType(), True),
                ]
            ),
        )

        df = df.withColumn(
            "observations",
            F.from_json(
                F.col("json_data"),
                schema=StructType(
                    [
                        StructField("realtime_start", StringType(), True),
                        StructField("realtime_end", StringType(), True),
                        StructField("date", StringType(), True),
                        StructField("value", StringType(), True),
                    ]
                ),
            ),
        ).drop("json_data")

        expected_df = spark.createDataFrame(
            [
                (datetime.date(1929, 1, 1), 1065.9),
                (datetime.date(1930, 1, 1), 975.5),
            ],
            schema=StructType(
                [
                    StructField("date", DateType(), True),
                    StructField("rate", DoubleType(), True),
                ]
            ),
        )

        df = df.transform(select_column)

        chispa.assert_df_equality(df, expected_df, ignore_nullable=True)

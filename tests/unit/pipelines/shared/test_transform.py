import datetime

import chispa
import pyspark.sql.functions as F
from pyspark.sql.types import (DateType, StringType, StructField, StructType,
                               TimestampType)

from pipelines.shared.transform import (add_load_dttm, add_load_prdt,
                                        add_md5_hash)


class TestTransform:
    def test_add_md5_hash(self, spark):
        df = spark.createDataFrame(
            [('{"symbol": "BTC","price": 60000}', "357a42ac565d120f9ac24a1aa5b29473")],
            schema=StructType(
                [
                    StructField("json_data", StringType(), True),
                    StructField("expected_md5_hash_value", StringType(), True),
                ]
            ),
        )

        df = df.transform(add_md5_hash, col="json_data")
        chispa.assert_column_equality(df, "md5_hash_value", "expected_md5_hash_value")

    def test_add_load_dttm(self, spark, mocker):
        FIXED_TIMESTAMP = F.lit("2026-01-01T10:00:00+00:00").cast(TimestampType())
        df = spark.createDataFrame(
            [('{"symbol": "BTC","price": 60000}',)],
            schema=StructType([StructField("json_data", StringType(), True)]),
        )
        df = df.withColumn("expected_load_dttm", FIXED_TIMESTAMP)
        mocker.patch(
            "pyspark.sql.functions.current_timestamp", return_value=FIXED_TIMESTAMP
        )
        df = df.transform(add_load_dttm)
        chispa.assert_column_equality(df, "load_dttm", "expected_load_dttm")

    def test_add_load_prdt(self, spark):
        df = spark.createDataFrame(
            [
                (
                    '{"symbol": "BTC","price": 60000}',
                    datetime.datetime(2026, 1, 1, 10, 0, 0),
                )
            ],
            schema=StructType(
                [
                    StructField("json_data", StringType(), True),
                    StructField("load_dttm", TimestampType(), True),
                ]
            ),
        )

        expected_load_prdt = F.lit("2026-01-01").cast(DateType())
        df = df.withColumn("expected_load_prdt", expected_load_prdt)
        df = df.transform(add_load_prdt, col="load_dttm")
        chispa.assert_column_equality(df, "load_prdt", "expected_load_prdt")

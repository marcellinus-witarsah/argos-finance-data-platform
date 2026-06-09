from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType
import pyspark.sql.functions as F
from tests.unit.pyspark_conftest import PysparkConftest
from pipelines.shared.transform import add_md5_hash, add_load_dttm, add_load_prdt
from unittest.mock import patch

import chispa

class TestTransform(PysparkConftest):
    def test_add_md5_hash(self):        
        df = self.spark.createDataFrame(
            [('{"symbol": "BTC","price": 60000}', "357a42ac565d120f9ac24a1aa5b29473")],
            schema=StructType([
                StructField("json_data", StringType(), True),
                StructField("expected_hash_id", StringType(), True),
            ]) 
        )

        df = df.transform(add_md5_hash, col=F.col("json_data"))
        chispa.assert_column_equality(df, "hash_id", "expected_hash_id")


    def test_add_load_dttm(self):
        FIXED_TIMESTAMP = F.lit("2026-01-01T10:00:00+00:00").cast(TimestampType())
        df = self.spark.createDataFrame(
            [('{"symbol": "BTC","price": 60000}',)],
            schema=StructType([StructField("json_data", StringType(), True)]) 
        )
        df = df.withColumn("expected_load_dttm", FIXED_TIMESTAMP)
        with patch("pyspark.sql.functions.current_timestamp", return_value=FIXED_TIMESTAMP):
            df = df.transform(add_load_dttm)
        chispa.assert_column_equality(df, "load_dttm", "expected_load_dttm")


    def test_add_load_prdt(self):
        FIXED_TIMESTAMP = F.lit("2026-01-01T10:00:00+00:00").cast(TimestampType())
        FIXED_DATE = F.lit("2026-01-01").cast(DateType())
        df = self.spark.createDataFrame(
            [('{"symbol": "BTC","price": 60000}',)],
            schema=StructType([
                StructField("json_data", StringType(), True),
            ]) 
        )
        df = df.withColumn("expected_load_prdt", FIXED_DATE)
        df = df.transform(add_load_prdt, col=FIXED_TIMESTAMP)
        chispa.assert_column_equality(df, "load_prdt", "expected_load_prdt")

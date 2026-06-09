import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame

from src.strategy.hasher.base_hasher_startegy import BaseHasherStrategy
from src.strategy.hasher.hasher_context import HasherContext


def add_md5_hash(
    df: DataFrame, col: Column | str
) -> DataFrame:
    return df.withColumn("hash_id", F.md5(col))


def add_load_dttm(df: DataFrame) -> DataFrame:
    return df.withColumn("load_dttm", F.current_timestamp())


def add_load_prdt(df: DataFrame, col: Column | str) -> DataFrame:
    return df.withColumn("load_prdt", F.to_date(col))

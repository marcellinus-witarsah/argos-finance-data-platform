import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


def add_md5_hash(df: DataFrame, col: str, target_col: str) -> DataFrame:
    return df.withColumn(target_col, F.md5(F.col(col)))


def parse_json(
    df: DataFrame, col: str, schema: StructType, target_col: str
) -> DataFrame:
    return df.withColumn(target_col, F.from_json(F.col(col), schema))


def add_load_dttm(df: DataFrame, target_col: str) -> DataFrame:
    return df.withColumn(target_col, F.current_timestamp())


def add_load_prdt(df: DataFrame, col: str, target_col: str) -> DataFrame:
    return df.withColumn(target_col, F.to_date(F.col(col)))

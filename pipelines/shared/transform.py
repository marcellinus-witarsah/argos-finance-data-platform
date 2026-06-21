import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def add_md5_hash(df: DataFrame, col: str) -> DataFrame:
    return df.withColumn("md5_hash_value", F.md5(F.col(col)))

def add_load_dttm(df: DataFrame) -> DataFrame:
    return df.withColumn("load_dttm", F.current_timestamp())

def add_load_prdt(df: DataFrame, col: str) -> DataFrame:
    return df.withColumn("load_prdt", F.to_date(F.col(col)))

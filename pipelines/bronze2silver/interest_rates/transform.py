import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DateType, DoubleType


def explode_json(df: DataFrame, col: str) -> DataFrame:
    return df.withColumn("observations", F.explode(F.col(col)["observations"])).drop(
        col, "json_data"
    )


def select_column(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("observations.date").cast(DateType()).alias("date"),
        F.col("observations.value").cast(DoubleType()).alias("rate"),
    )

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, DateType, DoubleType
from pyspark.sql import DataFrame


def parse_json(df: DataFrame, col: str, schema: StructType) -> DataFrame:
    return df.withColumn("parsed_json", F.from_json(F.col(col), schema))


def explode_json(df: DataFrame, col: str) -> DataFrame:
    return df.withColumn("observations", F.explode(F.col(col)["observations"])).drop(
        col, "json_data"
    )


def select_column(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("observations.date").cast(DateType()).alias("date"),
        F.col("observations.value").cast(DoubleType()).alias("rate"),
    )

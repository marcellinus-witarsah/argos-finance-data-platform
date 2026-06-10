import pyspark.sql.functions as F
from pyspark.sql.types import StructType, DateType, DoubleType
from pyspark.sql import DataFrame, Column


def parse_json(df: DataFrame, col: str, schema: StructType) -> DataFrame:
    return df.withColumn("parsed_json", F.from_json(F.col(col), schema))


def explode_json(df: DataFrame, col: str) -> DataFrame:
    return df.select(
        F.col(f"{col}.`Meta Data`.`2. Digital Currency Code`").alias("ticker"),
        F.col(f"{col}.`Meta Data`.`4. Market Code`").alias("currency"),
        F.explode(f"{col}.`Time Series (Digital Currency Daily)`").alias(
            "date", "ohlcv"
        ),
    )


def select_column(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("ticker"),
        F.col("currency"),
        F.col("date").cast(DateType()).alias("date"),
        F.col("ohlcv.`1. open`").cast(DoubleType()).alias("open"),
        F.col("ohlcv.`2. high`").cast(DoubleType()).alias("high"),
        F.col("ohlcv.`3. low`").cast(DoubleType()).alias("low"),
        F.col("ohlcv.`4. close`").cast(DoubleType()).alias("close"),
        F.col("ohlcv.`5. volume`").cast(DoubleType()).alias("volume"),
    )

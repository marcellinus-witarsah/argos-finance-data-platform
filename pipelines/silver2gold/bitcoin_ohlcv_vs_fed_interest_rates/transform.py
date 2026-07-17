import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def select_column(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("silver_crypto_ohlcv.ticker").alias("ticker"),
        F.col("silver_crypto_ohlcv.currency").alias("currency"),
        F.col("silver_crypto_ohlcv.date").alias("date"),
        F.col("silver_crypto_ohlcv.open").alias("open"),
        F.col("silver_crypto_ohlcv.high").alias("high"),
        F.col("silver_crypto_ohlcv.low").alias("low"),
        F.col("silver_crypto_ohlcv.close").alias("close"),
        F.col("silver_crypto_ohlcv.volume").alias("volume"),
        F.col("silver_interest_rates.rate").alias("rate"),
    )

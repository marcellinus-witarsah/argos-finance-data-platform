import argparse
from typing import Any

import requests
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, explode, from_json
from pyspark.sql.types import (DateType, DoubleType, MapType, StringType,
                               StructField, StructType)

from src.extractor.dataframe_extractor import DataframeExtractor
from src.pipeline.base_pipeline import BasePipeline
from src.strategy.parser.parser_context import ParserContext
from src.strategy.parser.yaml_parser_strategy import YAMLParserStrategy
from src.utils.context import PipelineContext
from src.utils.logger import logger
from src.writer.iceberg_writer import IcebergWriter


class DataPipeline(BasePipeline):
    def extract(self) -> dict[str, Any]:
        """Return named DataFrames. Keys are used in transform()."""
        data = self.extractor.extract()
        return data

    def transform(self, sources: dict[str, Any]) -> dict[str, DataFrame]:
        """Custom Spark logic. Return named output DataFrames."""
        bronze = sources["bronze_alpha_vantage_crypto_ohlcv"]

        schema = StructType(
            [
                StructField("Meta Data", MapType(StringType(), StringType())),
                StructField(
                    "Time Series (Digital Currency Daily)",
                    MapType(
                        StringType(),
                        StructType(
                            [
                                StructField("1. open", StringType()),
                                StructField("2. high", StringType()),
                                StructField("3. low", StringType()),
                                StructField("4. close", StringType()),
                                StructField("5. volume", StringType()),
                            ]
                        ),
                    ),
                ),
            ]
        )

        parsed = bronze.filter(~col("json_data").contains("Error Message")).withColumn(
            "parsed", from_json("json_data", schema)
        )

        exploded = parsed.select(
            col("parsed.`Meta Data`.`2. Digital Currency Code`").alias("ticker"),
            col("parsed.`Meta Data`.`4. Market Code`").alias("currency"),
            explode("parsed.`Time Series (Digital Currency Daily)`").alias(
                "date", "ohlcv"
            ),
        )

        silver_crypto_ohlcv = exploded.select(
            col("ticker"),
            col("currency"),
            col("date").cast(DateType()).alias("date"),
            col("ohlcv.`1. open`").cast(DoubleType()).alias("open"),
            col("ohlcv.`2. high`").cast(DoubleType()).alias("high"),
            col("ohlcv.`3. low`").cast(DoubleType()).alias("low"),
            col("ohlcv.`4. close`").cast(DoubleType()).alias("close"),
            col("ohlcv.`5. volume`").cast(DoubleType()).alias("volume"),
        ).distinct()

        load_dttm = current_timestamp()
        silver_crypto_ohlcv = silver_crypto_ohlcv.withColumn("load_dttm", load_dttm)

        return silver_crypto_ohlcv

    def write(self, output: dict[str, DataFrame]) -> None:
        """Write each output to its sink."""
        self.writer.write(output)


if __name__ == "__main__":
    load_dotenv()

    # Get command line interface if any
    parser = argparse.ArgumentParser(description="Run the bronze data pipeline.")
    parameters = parser.parse_args()

    # Parse configuration file
    yaml_parser = ParserContext(YAMLParserStrategy(logger))
    cfg = yaml_parser.parse("./configs/bronze2silver/crypto_ohlcv.yaml")

    # Create context to be passed down to whole data pipeline
    ctx = PipelineContext(
        logger=logger,
        spark=SparkSession.builder.appName("crypto_ohlcv").getOrCreate(),
        session=requests.Session(),
    )

    # Create extractor
    cfg_extractor = cfg["extractor"]
    dataframe_extractor = DataframeExtractor(ctx, cfg_extractor)

    # Create writer
    cfg_writer = cfg["writer"]
    iceberg_writer = IcebergWriter(ctx, cfg_writer)

    # Create data pipeline and run it
    data_pipeline = DataPipeline(
        ctx=ctx, extractor=dataframe_extractor, writer=iceberg_writer, cfg=cfg
    )

    data_pipeline.run()

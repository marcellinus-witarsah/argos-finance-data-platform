import argparse
from typing import Any

import requests
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, explode, from_json
from pyspark.sql.types import (ArrayType, DateType, DoubleType, IntegerType,
                               StringType, StructField, StructType)

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
        bronze = sources["bronze_interest_rates"]

        schema = StructType(
            [
                StructField("realtime_start", StringType(), True),
                StructField("realtime_end", StringType(), True),
                StructField("observation_start", StringType(), True),
                StructField("observation_end", StringType(), True),
                StructField("units", StringType(), True),
                StructField("output_type", IntegerType(), True),
                StructField("file_type", StringType(), True),
                StructField("order_by", StringType(), True),
                StructField("sort_order", StringType(), True),
                StructField("count", IntegerType(), True),
                StructField("offset", IntegerType(), True),
                StructField("limit", IntegerType(), True),
                StructField(
                    "observations",
                    ArrayType(
                        StructType(
                            [
                                StructField("realtime_start", StringType(), True),
                                StructField("realtime_end", StringType(), True),
                                StructField("date", StringType(), True),
                                StructField("value", StringType(), True),
                            ]
                        )
                    ),
                    True,
                ),
            ]
        )

        parsed = bronze.filter(~col("json_data").contains("Error Message")).withColumn(
            "parsed", from_json("json_data", schema)
        )

        exploded = parsed.select(explode("parsed.observations").alias("observation"))

        silver_df = exploded.select(
            col("observation.date").cast(DateType()).alias("date"),
            col("observation.value").cast(DoubleType()).alias("rate"),
        ).distinct()

        load_dttm = current_timestamp()
        silver_df = silver_df.withColumn("load_dttm", load_dttm)

        return silver_df

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
    cfg = yaml_parser.parse("./configs/bronze2silver/interest_rates.yaml")

    # Create context to be passed down to whole data pipeline
    ctx = PipelineContext(
        logger=logger,
        spark=SparkSession.builder.appName("interest_rates").getOrCreate(),
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

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import to_date, current_timestamp
from src.pipeline.base_pipeline import BasePipeline
from src.strategy.parser.parser_context import ParserContext
from src.strategy.parser.yaml_parser_strategy import YAMLParserStrategy
from src.strategy.hasher.hasher_context import HasherContext
from src.strategy.hasher.md5_hasher_strategy import MD5HasherStrategy
from typing import Any
import os
import json
from src.utils.context import PipelineContext
from src.utils.logger import logger
from pyspark.sql import SparkSession
import requests

from src.extractor.api_extractor import APIExtractor
import argparse
from dotenv import load_dotenv
from src.writer.iceberg_writer import IcebergWriter


class FREDInterestRatesPipeline(BasePipeline):
    def extract(self) -> dict[str, Any]:
        """Return named DataFrames. Keys are used in transform()."""
        data = self.extractor.extract()
        return {"data": data}

    def transform(self, sources: dict[str, Any]) -> dict[str, DataFrame]:
        """Custom Spark logic. Return named output DataFrames."""
        data = sources["data"]
        hasher_context = HasherContext(strategy=MD5HasherStrategy())
        id = hasher_context.hash(json.dumps(data))

        df = self.spark.createDataFrame(
            data=[
                (id, json.dumps(data)),
            ],
            schema=StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("json_data", StringType(), False),
                ]
            ),
        )

        load_dttm = current_timestamp()
        df = df.withColumn("load_dttm", load_dttm)
        df = df.withColumn("load_prdt", to_date(load_dttm))

        return df

    def write(self, output: dict[str, DataFrame]) -> None:
        """Write each output to its sink."""
        self.writer.write(output)

    def run(self) -> None:
        sources = self.extract()
        output = self.transform(sources)
        self.write(output)


if __name__ == "__main__":
    load_dotenv()

    # Get command line interface if any
    parser = argparse.ArgumentParser(description="Run the bronze data pipeline.")
    parameters = parser.parse_args()

    # Parse configuration file
    yaml_parser = ParserContext(YAMLParserStrategy())
    cfg = yaml_parser.parse("./configs/api2bronze/fred/interest_rates.yaml")

    # Create context to be passed down to whole data pipeline
    ctx = PipelineContext(
        logger=logger,
        spark=SparkSession.builder.appName("alpha_vantage_crypto_ohlcv").getOrCreate(),
        session=requests.Session(),
    )

    # Create extractor
    cfg_extractor = cfg["extractor"]
    cfg_extractor["query_params"] = {
        **cfg_extractor["query_params"],
        "apikey": os.getenv("FRED_API_KEY"),
    }
    api_extractor = APIExtractor(ctx, cfg_extractor)

    # Create writer
    cfg_writer = cfg["writer"]
    iceberg_writer = IcebergWriter(ctx, cfg_writer)

    # Create data pipeline and run it
    data_pipeline = FREDInterestRatesPipeline(
        ctx=ctx, extractor=api_extractor, writer=iceberg_writer, cfg=cfg
    )

    data_pipeline.run()

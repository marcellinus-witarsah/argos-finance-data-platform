import argparse
import json
import os
from typing import Sequence

import requests
from dotenv import load_dotenv
from pyspark.sql.types import StringType, StructField, StructType

from pipelines.shared.transform import (add_load_dttm, add_load_prdt,
                                        add_md5_hash)
from src.extractor.api_extractor import APIExtractor
from src.strategy.parser.parser_context import ParserContext
from src.strategy.parser.yaml_parser_strategy import YAMLParserStrategy
from src.utils.spark_session import get_spark
from src.writer.spark_dataframe_writer import SparkDataframeWriter


def get_parameters(argv: None | Sequence = None):
    parser = argparse.ArgumentParser(description="Run the bronze data pipeline.")
    parser.add_argument("--symbol", type=str, required=True)
    parser.add_argument("--market", type=str, required=True)
    return parser.parse_args(argv)


def get_configuration(parameters: argparse.Namespace, cfg: dict, env: dict) -> dict:
    api_key = env.get("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        raise ValueError("ALPHA_VANTAGE_API_KEY environment variable is not set")

    cfg["extractor"]["query_params"].update(
        {
            "market": parameters.market,
            "symbol": parameters.symbol,
            "apikey": api_key,
        }
    )
    return cfg


def run(cfg: dict):
    # Get Spark Session and Requests Session
    spark = get_spark()
    session = requests.Session()

    # Extract
    extractor_cfg = cfg.get("extractor", {})
    api_extractor = APIExtractor(session=session)
    bronze_alpha_vantage_crypto_ohlcv_data = api_extractor.extract(
        url=extractor_cfg.get("url", ""),
        query_params=extractor_cfg.get("query_params", {}),
        headers=extractor_cfg.get("headers", {}),
    )

    # Transform
    df = spark.createDataFrame(
        data=[(json.dumps(bronze_alpha_vantage_crypto_ohlcv_data),)],
        schema=StructType(
            [
                StructField("json_data", StringType(), False),
            ]
        ),
    )

    df = (
        df.transform(add_md5_hash, col="json_data", target_col="id")
        .transform(add_load_dttm, target_col="load_dttm")
        .transform(add_load_prdt, col="load_dttm", target_col="load_prdt")
    )

    # Load
    writer_cfg = cfg.get("writer", {})
    spark_data_frame_writer = SparkDataframeWriter(spark=spark)
    spark_data_frame_writer.write(
        df=df,
        fmt=writer_cfg.get("fmt", ""),
        table=writer_cfg.get("table", ""),
        mode=writer_cfg.get("mode", ""),
        merge_columns=writer_cfg.get("merge_columns", []),
        partition_columns=writer_cfg.get("partition_columns", []),
    )


def main():
    # Load environment variables
    load_dotenv()

    # Get pipeline parameters
    parameters = get_parameters()

    # Read YAML file
    yaml_parser = ParserContext(YAMLParserStrategy())
    cfg = yaml_parser.parse("./configs/api2bronze/alpha_vantage_crypto_ohlcv.yaml")

    # Get pipeline configuration
    cfg = get_configuration(parameters=parameters, cfg=cfg, env=os.environ)

    # Run pipeline
    run(cfg=cfg)


if __name__ == "__main__":
    main()

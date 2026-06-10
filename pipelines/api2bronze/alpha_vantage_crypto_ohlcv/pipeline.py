import argparse
import json
import os

import requests
from dotenv import load_dotenv
from pyspark.sql.types import StringType, StructField, StructType

from pipelines.shared.transform import (
    add_hash_id_column,
    add_load_dttm_column,
    add_load_prdt_column,
)
from src.extractor.api_extractor import APIExtractor
from src.strategy.hasher.md5_hasher_strategy import MD5HasherStrategy
from src.strategy.parser.parser_context import ParserContext
from src.strategy.parser.yaml_parser_strategy import YAMLParserStrategy
from src.utils.spark_session import spark
from src.writer.spark_dataframe_writer import SparkDataframeWriter


def get_configuration() -> dict:
    parser = argparse.ArgumentParser(description="Run the bronze data pipeline.")
    parser.add_argument("--symbol", type=str, required=True)
    parser.add_argument("--market", type=str, required=True)
    parameters = parser.parse_args()

    yaml_parser = ParserContext(YAMLParserStrategy())
    cfg = yaml_parser.parse("./configs/api2bronze/alpha_vantage_crypto_ohlcv.yaml")

    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
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
    # Extract
    session = requests.Session()
    extractor_cfg = cfg.get("extractor", {})
    bronze_alpha_vantage_crypto_ohlcv_data = APIExtractor.extract(
        session=session,
        url=extractor_cfg.get("url", ""),
        query_params=extractor_cfg.get("query_params", {}),
        headers=extractor_cfg.get("headers", {}),
    )

    # Transform
    df = spark.createDataFrame(
        data=[json.dumps(bronze_alpha_vantage_crypto_ohlcv_data)],
        schema=StructType(
            [
                StructField("json_data", StringType(), False),
            ]
        ),
    )

    df = (
        df.transform(
            add_hash_id_column, col="json_data", hasher_stategy=MD5HasherStrategy()
        )
        .transform(add_load_dttm_column)
        .transform(add_load_prdt_column)
    )

    # Load
    writer_cfg = cfg.get("writer", {})
    SparkDataframeWriter.write(
        spark=spark,
        df=df,
        fmt=writer_cfg.get("fmt", ""),
        table=writer_cfg.get("table", ""),
        mode=writer_cfg.get("mode", ""),
        merge_columns=writer_cfg.get("merge_columns", []),
        partition_columns=writer_cfg.get("partition_columns", []),
    )


def main():
    load_dotenv()
    cfg = get_configuration()
    run(cfg=cfg)


if __name__ == "__main__":
    main()

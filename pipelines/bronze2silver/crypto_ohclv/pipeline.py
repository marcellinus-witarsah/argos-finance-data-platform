import json
import os

import requests
from dotenv import load_dotenv
from pyspark.sql.types import StringType, StructField, StructType, MapType

from pipelines.shared.transform import (
    add_hash_id_column,
    add_load_dttm_column,
    add_load_prdt_column,
)
from pipelines.bronze2silver.crypto_ohclv.transform import (
    parse_json,
    explode_json,
    select_column,
)
from src.extractor.spark_dataframe_extractor import SparkDataframeExtractor
from src.strategy.hasher.md5_hasher_strategy import MD5HasherStrategy
from src.strategy.parser.parser_context import ParserContext
from src.strategy.parser.yaml_parser_strategy import YAMLParserStrategy
from src.utils.spark_session import spark
from src.writer.spark_dataframe_writer import SparkDataframeWriter


def get_configuration() -> dict:
    yaml_parser = ParserContext(YAMLParserStrategy())
    cfg = yaml_parser.parse("./configs/bronze2silver/crypto_ohlcv.yaml")
    return cfg


def run(cfg: dict):
    # Extract
    bronze_alpha_vantage_crypto_ohlcv_df = SparkDataframeExtractor.extract(
        spark=spark,
        table="catalog.argos_finance_catalog.bronze.alpha_vantage_crypto_ohlcv",
    )

    # Transform
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

    silver_crypto_ohlcv_df = (
        bronze_alpha_vantage_crypto_ohlcv_df.transform(
            parse_json, col="json_data", schema=schema, target_col="parsed_json_data"
        )
        .transform(explode_json, col="parsed_json_data")
        .transform(select_column)
        .transform(add_load_dttm_column)
        .transform(add_load_prdt_column)
        .distinct()
    )

    # Load
    writer_cfg = cfg.get("writer", {})
    SparkDataframeWriter.write(
        spark=spark,
        df=silver_crypto_ohlcv_df,
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

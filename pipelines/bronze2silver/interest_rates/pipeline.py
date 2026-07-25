import argparse
import json
import os
from typing import Sequence

import requests
from dotenv import load_dotenv
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from pipelines.bronze2silver.interest_rates.transform import explode_json, select_column
from pipelines.shared.transform import add_load_dttm, add_load_prdt, parse_json
from src.extractor.spark_dataframe_extractor import SparkDataframeExtractor
from src.strategy.parser.parser_context import ParserContext
from src.strategy.parser.yaml_parser_strategy import YAMLParserStrategy
from src.utils.spark_session import get_spark
from src.writer.spark_dataframe_writer import SparkDataframeWriter


def get_parameters(argv: None | Sequence = None):
    parser = argparse.ArgumentParser(description="Run the silver data pipeline.")
    parser.add_argument("--conf-yaml-file", type=str, required=True)
    return parser.parse_args(argv)


def get_configuration(conf_yaml_file: str) -> dict:
    yaml_parser = ParserContext(YAMLParserStrategy())
    cfg = yaml_parser.parse(conf_yaml_file)
    return cfg


def run(cfg: dict):
    # Extract
    spark = get_spark()
    spark_dataframe_extractor = SparkDataframeExtractor(spark=spark)
    bronze_fed_interest_rates_df = spark_dataframe_extractor.extract(
        table="argos_finance_catalog.bronze.fed_interest_rates"
    )

    # Transform
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

    silver_interest_rates_df = (
        bronze_fed_interest_rates_df.transform(
            parse_json, col="json_data", schema=schema, target_col="parsed_json_data"
        )
        .transform(explode_json, col="parsed_json_data")
        .transform(select_column)
        .transform(add_load_dttm, target_col="load_dttm")
        .transform(add_load_prdt, col="load_dttm", target_col="load_prdt")
        .distinct()
    )

    # Load
    writer_cfg = cfg.get("writer", {})
    spark_dataframe_writer = SparkDataframeWriter(spark=spark)
    spark_dataframe_writer.write(
        df=silver_interest_rates_df,
        fmt=writer_cfg.get("fmt", ""),
        table=writer_cfg.get("table", ""),
        mode=writer_cfg.get("mode", ""),
        merge_columns=writer_cfg.get("merge_columns", []),
        partition_columns=writer_cfg.get("partition_columns", []),
    )


def main():
    load_dotenv()
    parameters = get_parameters()
    cfg = get_configuration(conf_yaml_file=parameters.conf_yaml_file)
    run(cfg=cfg)


if __name__ == "__main__":
    main()

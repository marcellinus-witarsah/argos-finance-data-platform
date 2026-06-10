import json
import os

import requests
from dotenv import load_dotenv
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
    ArrayType,
)

from pipelines.shared.transform import (
    add_hash_id_column,
    add_load_dttm_column,
    add_load_prdt_column,
)
from pipelines.bronze2silver.interest_rates.transform import (
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
    cfg = yaml_parser.parse("./configs/bronze2silver/interest_rates.yaml")
    return cfg


def run(cfg: dict):
    # Extract
    bronze_fed_interest_rates_df = SparkDataframeExtractor.extract(
        spark=spark, table="catalog.argos_finance_catalog.bronze.fed_interest_rates"
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
        .transform(add_load_dttm_column)
        .transform(add_load_prdt_column)
        .distinct()
    )

    # Load
    writer_cfg = cfg.get("writer", {})
    SparkDataframeWriter.write(
        spark=spark,
        df=silver_interest_rates_df,
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

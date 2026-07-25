import argparse
from typing import Sequence

import pyspark.sql.functions as F
from dotenv import load_dotenv

from pipelines.shared.transform import add_load_dttm, add_load_prdt
from pipelines.silver2gold.bitcoin_ohlcv_vs_fed_interest_rates.transform import (
    select_column,
)
from src.extractor.spark_dataframe_extractor import SparkDataframeExtractor
from src.strategy.parser.parser_context import ParserContext
from src.strategy.parser.yaml_parser_strategy import YAMLParserStrategy
from src.utils.spark_session import get_spark
from src.writer.spark_dataframe_writer import SparkDataframeWriter


def get_parameters(argv: None | Sequence = None):
    parser = argparse.ArgumentParser(description="Run the gold data pipeline.")
    parser.add_argument("--conf-yaml-file", type=str, required=True)
    return parser.parse_args(argv)


def get_configuration(conf_yaml_file: str) -> dict:
    yaml_parser = ParserContext(YAMLParserStrategy())
    cfg = yaml_parser.parse(conf_yaml_file)
    return cfg


def run(cfg: dict):
    # Get Spark Session and Requests Session
    spark = get_spark()

    # Extract
    spark_dataframe_extractor = SparkDataframeExtractor(spark=spark)
    silver_crypto_ohlcv_df = spark_dataframe_extractor.extract(
        table="argos_finance_catalog.silver.crypto_ohlcv",
    )
    silver_interest_rates_df = spark_dataframe_extractor.extract(
        table="argos_finance_catalog.silver.interest_rates",
    )

    # Transform

    gold_df = (
        silver_crypto_ohlcv_df.alias("silver_crypto_ohlcv")
        .join(
            silver_interest_rates_df.alias("silver_interest_rates"),
            (
                F.month(F.col("silver_crypto_ohlcv.date"))
                == F.month(F.col("silver_interest_rates.date"))
            )
            & (
                F.year(F.col("silver_crypto_ohlcv.date"))
                == F.year(F.col("silver_interest_rates.date"))
            ),
        )
        .transform(select_column)
        .transform(add_load_dttm, target_col="load_dttm")
        .transform(add_load_prdt, col="load_dttm", target_col="load_prdt")
        .distinct()
    )

    # Load
    writer_cfg = cfg.get("writer", {})
    spark_dataframe_writer = SparkDataframeWriter(spark=spark)
    spark_dataframe_writer.write(
        df=gold_df,
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

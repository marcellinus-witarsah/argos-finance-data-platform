import json
import os

import requests
from dotenv import load_dotenv
from pyspark.sql.types import StringType, StructField, StructType

from pipelines.shared.transform import (
    add_md5_hash,
    add_load_dttm,
    add_load_prdt,
)
from src.extractor.api_extractor import APIExtractor
from src.strategy.parser.parser_context import ParserContext
from src.strategy.parser.yaml_parser_strategy import YAMLParserStrategy
from src.utils.spark_session import spark
from src.writer.spark_dataframe_writer import SparkDataframeWriter

def get_configuration(
        cfg: dict,
        env: dict
) -> dict:
    api_key = env.get("FRED_API_KEY")
    if not api_key:
        raise ValueError("FRED_API_KEY environment variable is not set")
    cfg["extractor"]["query_params"]["api_key"] = api_key
    return cfg

def run(cfg: dict):
    # Extract
    session = requests.Session()
    extractor_cfg = cfg.get("extractor", {})
    bronze_fed_interest_rates = APIExtractor.extract(
        session=session,
        url=extractor_cfg.get("url", ""),
        query_params=extractor_cfg.get("query_params", {}),
        headers=extractor_cfg.get("headers", {}),
    )

    # Transform
    df = spark.createDataFrame(
        data=[(json.dumps(bronze_fed_interest_rates),)],
        schema=StructType(
            [
                StructField("json_data", StringType(), False),
            ]
        ),
    )

    df = (
        df.transform(
            add_md5_hash, col="json_data"
        )
        .transform(add_load_dttm)
        .transform(add_load_prdt)
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
    # Load environment variables
    load_dotenv()

    # Read YAML file
    yaml_parser = ParserContext(YAMLParserStrategy())
    cfg = yaml_parser.parse("./configs/api2bronze/fed_interest_rates.yaml")

    # Get configuration
    cfg = get_configuration(cfg=cfg, env=os.environ)
    
    # Run pipeline
    run(cfg=cfg)


if __name__ == "__main__":
    main()

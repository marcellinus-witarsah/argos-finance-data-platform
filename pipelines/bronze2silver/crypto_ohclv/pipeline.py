from dotenv import load_dotenv
from pyspark.sql.types import MapType, StringType, StructField, StructType

from pipelines.bronze2silver.crypto_ohclv.transform import explode_json, select_column
from pipelines.shared.transform import add_load_dttm, add_load_prdt, parse_json
from src.extractor.spark_dataframe_extractor import SparkDataframeExtractor
from src.strategy.parser.parser_context import ParserContext
from src.strategy.parser.yaml_parser_strategy import YAMLParserStrategy
from src.utils.spark_session import get_spark
from src.writer.spark_dataframe_writer import SparkDataframeWriter


def get_configuration() -> dict:
    yaml_parser = ParserContext(YAMLParserStrategy())
    cfg = yaml_parser.parse("./configs/bronze2silver/crypto_ohlcv.yaml")
    return cfg


def run(cfg: dict):
    # Get Spark Session and Requests Session
    spark = get_spark()

    # Extract
    spark_dataframe_extractor = SparkDataframeExtractor(spark=spark)
    bronze_alpha_vantage_crypto_ohlcv_df = spark_dataframe_extractor.extract(
        table="argos_finance_catalog.bronze.alpha_vantage_crypto_ohlcv",
    )

    # Transform
    schema = StructType(
        [
            StructField(
                "Meta Data",
                StructType(
                    [
                        StructField("1. Information", StringType()),
                        StructField("2. Digital Currency Code", StringType()),
                        StructField("3. Digital Currency Name", StringType()),
                        StructField("4. Market Code", StringType()),
                        StructField("5. Market Name", StringType()),
                        StructField("6. Last Refreshed", StringType()),
                        StructField("7. Time Zone", StringType()),
                    ]
                ),
            ),
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
        .transform(add_load_dttm, target_col="load_dttm")
        .transform(add_load_prdt, col="load_dttm", target_col="load_prdt")
        .distinct()
    )

    # Load
    writer_cfg = cfg.get("writer", {})
    spark_dataframe_writer = SparkDataframeWriter(spark=spark)
    spark_dataframe_writer.write(
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

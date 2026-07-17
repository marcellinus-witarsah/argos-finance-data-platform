import shutil
import unittest
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.utils.logger import logger


@pytest.fixture(scope="session")
def spark():
    logger.info("Create Spark Session for Unit Testing ...")
    folder = Path("./warehouse")
    folder.mkdir(parents=True, exist_ok=True)
    spark = (
        SparkSession.builder.config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.argos_finance_catalog",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.argos_finance_catalog.type", "hadoop")
        .config("spark.sql.catalog.argos_finance_catalog.warehouse", "./warehouse")
        .config("spark.sql.catalog.defaultCatalog", "argos_finance_catalog")
        .config(
            "spark.sql.catalog.local.default.write.metadata-flush-after-create", "true"
        )
        .master("local[1]")
        .appName("UnitTest")
        .getOrCreate()
    )
    schemas = ["bronze", "silver", "gold"]
    for schema in schemas:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
    yield spark
    logger.info("Stop Spark Session after Unit Testing ...")
    shutil.rmtree("./warehouse")
    spark.stop()

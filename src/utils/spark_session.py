from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv, find_dotenv

# Load environment variables into application environment variable
load_dotenv(find_dotenv())

# Create a spark session
def get_spark() -> SparkSession:
    return SparkSession.builder.appName(
        os.getenv("PROJECT_NAME", "argos-finance-data-platform")
    ).getOrCreate()

# spark = (
#     SparkSession.builder
#     .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0")
#     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
#     .config("spark.sql.catalog.argos_finance_catalog", "org.apache.iceberg.spark.SparkCatalog")
#     .config("spark.sql.catalog.argos_finance_catalog.type", "hadoop")
#     .config("spark.sql.catalog.argos_finance_catalog.warehouse", "./warehouse")
#     .config("spark.sql.catalog.defaultCatalog", "argos_finance_catalog")
#     .config("spark.sql.catalog.local.default.write.metadata-flush-after-create", "true")
#     .master("local[1]")
#     .appName("UnitTest")
#     .getOrCreate()
# )

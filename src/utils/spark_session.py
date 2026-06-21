from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv, find_dotenv

# Load environment variables into application environment variable
load_dotenv(find_dotenv())

# Create a spark session
spark = SparkSession.builder.appName(
    os.getenv("PROJECT_NAME", "argos-finance-data-platform")
).getOrCreate()

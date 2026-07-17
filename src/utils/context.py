from dataclasses import dataclass
import logging
from pyspark.sql import SparkSession
import requests


@dataclass
class PipelineContext:
    logger: logging.Logger
    spark: SparkSession | None = None
    session: requests.Session | None = None

# framework/pipeline/base.py
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from src.utils.context import PipelineContext
from src.extractor.base_extractor import BaseExtractor
from src.writer.base_writer import BaseWriter
from typing import Any

class BasePipeline(ABC):
    def __init__(self, ctx: PipelineContext, extractor: BaseExtractor, writer: BaseWriter, cfg: dict):
        self.spark = ctx.spark
        self.logger = ctx.logger
        self.session = ctx.session
        self.extractor = extractor
        self.writer = writer
        self.cfg = cfg

    @abstractmethod
    def extract(self) -> dict[str, Any]:
        """Return named DataFrames. Keys are used in transform()."""
        pass

    @abstractmethod
    def transform(self, sources: dict[str, (dict | DataFrame)]) -> DataFrame:
        """Custom Spark logic. Return named output DataFrames."""
        pass

    @abstractmethod
    def write(self, output: DataFrame) -> None:
        """Write each output to its sink."""
        pass

    def run(self) -> None:
        sources = self.extract()
        outputs = self.transform(sources)
        self.write(outputs)
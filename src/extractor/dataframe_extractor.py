# ================================================================================
# Author: Marcellinus A.W.
# Date: 16 May 2026
# Description: Decided to create an abstract class for extractor so that each new
# extractor has to inherite from this base class
# ================================================================================

from src.extractor.base_extractor import BaseExtractor


class DataframeExtractor(BaseExtractor):

    def extract(self):
        catalog = self.cfg.get("catalog", "")
        schema = self.cfg.get("schema", "")
        tables = self.cfg.get("table", [])
        data = {}
        for table in tables:
            df = self.ctx.spark.read.table(f"{catalog}.{schema}.{table}")
            data[f"{schema}_{table}"] = df
            self.ctx.logger.info(f"Extract data from {catalog}.{schema}.{table} table")
        return data

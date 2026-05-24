from src.extractor.base_extractor import BaseExtractor
from typing import Any
import requests


class APIExtractor(BaseExtractor):
    def extract(
        self,
    ) -> (
        Any
    ):  # expected data type result `Any` because we will be pulling data from all APIs with variety of response format
        try:
            response = self.ctx.session.get(
                url=self.cfg.get("url", ""),
                params=self.cfg.get("query_params", {}),
                headers=self.cfg.get("headers", {}),
            )
            self.ctx.logger.info(self.cfg)
        except requests.exceptions.HTTPError as e:
            self.ctx.logger.error(e)
        except Exception as e:
            self.ctx.logger.error(e)
        self.ctx.logger.info(f"Extract data via API from {self.cfg.get('url', '')}.")
        return response.json()

from src.extractor.base_extractor import BaseExtractor
import requests
from src.utils.logger import logger


class APIExtractor(BaseExtractor):
    @staticmethod
    def extract(session: requests.Session, url: str, query_params: dict, headers: dict) -> dict:
        logger.info(f"Extracting data via API from {url}.")
        response = session.get(
            url=url,
            params=query_params,
            headers=headers,
        )
        response.raise_for_status()
        logger.info(f"Extracted data via API successfull.")
        return response.json()

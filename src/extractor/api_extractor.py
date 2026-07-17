from src.extractor.base_extractor import BaseExtractor
import requests
from src.utils.logger import logger


class APIExtractor(BaseExtractor):
    def __init__(self, session: requests.Session):
        self.session = session
    
    def extract(
        self, url: str, query_params: dict, headers: dict
    ) -> dict:
        logger.info(f"Extracting data via API from {url}.")
        response = self.session.get(
            url=url,
            params=query_params,
            headers=headers,
        )
        response.raise_for_status()
        logger.info(f"Extracted data via API successfull.")
        return response.json()

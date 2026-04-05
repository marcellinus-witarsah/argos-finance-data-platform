import json

import requests

from src.ingestion.api.base_api import BaseAPI

BASE_URL = "https://www.alphavantage.co"


class AlphaVantageAPI(BaseAPI):
    def __init__(self, api_key):
        self.api_key = api_key

    def fetch_ohlcv(self, params: dict) -> json:
        """Fetch OHLCV data for a given cryptocurrency symbol and market."""
        url = f"{BASE_URL}/query?function=DIGITAL_CURRENCY_DAILY&symbol={params['symbol']}&market={params['market']}&apikey={self.api_key}"
        response = requests.get(url)
        return response.json()

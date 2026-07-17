import re

import pytest
import requests

from src.extractor.api_extractor import APIExtractor


class MockResponse:
    def __init__(self, status_code: str, data: dict):
        self.status_code = status_code
        self.data = data

    def json(self):
        return self.data

    def raise_for_status(self):
        error_status_code_pattern = r"^[45]\d{2}$"
        if re.search(error_status_code_pattern, self.status_code):
            raise requests.exceptions.HTTPError


class TestAPIExtractor:
    def test_api_extractor_returns_dict_data_type_on_success(self, mocker):
        mock_session = mocker.Mock()
        mock_url = "https://www.alphavantage.co/query"
        mock_query_params = {
            "function": "DIGITAL_CURRENCY_DAILY",
            "symbol": "BTC",
            "market": "USD",
            "apikey": "mock_api",
        }
        mock_headers = {}

        mock_session.get.return_value = MockResponse(
            status_code="200",
            data={"date": "2026-05-27", "symbol": "BTC", "price": 74000},
        )
        api_extractor = APIExtractor(mock_session)
        data = api_extractor.extract(mock_url, mock_query_params, mock_headers)

        assert isinstance(data, dict)

    def test_api_extractor_returns_json_data_on_success(self, mocker):
        mock_session = mocker.Mock()
        mock_url = "https://www.alphavantage.co/query"
        mock_query_params = {
            "function": "DIGITAL_CURRENCY_DAILY",
            "symbol": "BTC",
            "market": "USD",
            "apikey": "mock_api",
        }
        mock_headers = {}

        mock_session.get.return_value = MockResponse(
            status_code="200",
            data={"date": "2026-05-27", "symbol": "BTC", "price": 74000},
        )
        api_extractor = APIExtractor(mock_session)
        data = api_extractor.extract(mock_url, mock_query_params, mock_headers)

        assert data == {"date": "2026-05-27", "symbol": "BTC", "price": 74000}

    def test_api_extractor_raise_http_error_on_failed(self, mocker):
        mock_session = mocker.Mock()
        mock_url = "https://www.alphavantage.co/query"
        mock_query_params = {
            "function": "DIGITAL_CURRENCY_DAILY",
            "symbol": "BTC",
            "market": "USD",
            "apikey": "mock_api",
        }
        mock_headers = {}

        mock_session.get.return_value = MockResponse(status_code="500", data={})
        api_extractor = APIExtractor(mock_session)

        with pytest.raises(requests.exceptions.HTTPError):
            api_extractor.extract(mock_url, mock_query_params, mock_headers)

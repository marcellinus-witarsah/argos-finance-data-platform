from unittest import mock, TestCase
from src.extractor.api_extractor import APIExtractor
import requests
import re


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


class TestAPIExtractor(TestCase):
    def setUp(self):
        """A function that always runs at the start of unit test."""
        self.mock_session = mock.Mock(spec=requests.Session)
        self.mock_url = "https://www.alphavantage.co/query"
        self.mock_query_params = {
            "function": "DIGITAL_CURRENCY_DAILY",
            "symbol": "BTC",
            "market": "USD",
            "apikey": "mock_api",
        }
        self.mock_headers = {}

    def tearDown(self):
        """A function that always runs after unit test function is run."""
        pass

    def test_api_extractor_returns_dict_data_type_on_success(self):
        self.mock_session.get.return_value = MockResponse(
            status_code="200",
            data={"date": "2026-05-27", "symbol": "BTC", "price": 74000},
        )
        data = APIExtractor.extract(
            self.mock_session, self.mock_url, self.mock_query_params, self.mock_headers
        )
        self.assertIsInstance(data, dict)

    def test_api_extractor_returns_json_data_on_success(self):
        self.mock_session.get.return_value = MockResponse(
            status_code="200",
            data={"date": "2026-05-27", "symbol": "BTC", "price": 74000},
        )
        data = APIExtractor.extract(
            self.mock_session, self.mock_url, self.mock_query_params, self.mock_headers
        )
        self.assertEqual(data, {"date": "2026-05-27", "symbol": "BTC", "price": 74000})

    def test_api_extractor_raise_http_error_on_failed(self):
        self.mock_session.get.return_value = MockResponse(status_code="500", data={})
        with self.assertRaises(requests.exceptions.HTTPError):
            APIExtractor.extract(
                self.mock_session,
                self.mock_url,
                self.mock_query_params,
                self.mock_headers,
            )

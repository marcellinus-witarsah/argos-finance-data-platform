from src.ingestion.api.alpha_vantage_api import AlphaVantageAPI
from unittest import mock


def test_api_key_attribute_has_expected_value_after_init():
    mock_api_key = "alpha_vantage_api_key"
    expected_api_key = mock_api_key

    alpha_vantage_api = AlphaVantageAPI(api_key=mock_api_key)

    assert alpha_vantage_api.api_key == expected_api_key


@mock.patch("src.ingestion.api.alpha_vantage_api.requests.get")
def test_fetch_ohlcv_method_has_expected_value_after_call(requests_get_method):
    mock_api_key = "alpha_vantage_api_key"
    mock_data = {
        "Meta Data": {
            "1. Information": "Daily Prices and Volumes for Digital Currency",
            "2. Digital Currency Code": "BTC",
            "3. Digital Currency Name": "Bitcoin",
            "4. Market Code": "EUR",
            "5. Market Name": "Euro",
            "6. Last Refreshed": "2026-04-05 00:00:00",
            "7. Time Zone": "UTC",
        },
        "Time Series (Digital Currency Daily)": {
            "2026-04-05": {
                "1. open": "58445.98000000",
                "2. high": "58456.19000000",
                "3. low": "58379.06000000",
                "4. close": "58414.54000000",
                "5. volume": "2.57032856",
            },
        },
    }
    expected_data = mock_data
    mock_response = mock.Mock()

    # Mock requests.get
    requests_get_method.return_value = mock_response

    # Mock response.json
    mock_response.json.return_value = mock_data

    # Insntatiate and run the function
    alpha_vantage_api = AlphaVantageAPI(api_key=mock_api_key)
    data = alpha_vantage_api.fetch_ohlcv(params={"symbol": "BTC", "market": "USD"})

    assert data == expected_data

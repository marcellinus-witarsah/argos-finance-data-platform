import unittest.mock as mock
from src.ingestion.api.fred import FredAPIClient

class FakeResponse:
    """
    Fake for requests.Response (or any HTTP response).
    Only implements what your code actually calls.
    """
    def __init__(
        self,
        data: dict | list | None = None,   # what .json() returns
        status_code: int = 200,
    ):
        self._data = data or {}
        self.status_code = status_code

    def json(self) -> dict:
        return self._data

    def __str__(self):
        return f"<Response [{self.status_code}]>"

def _make_client(session=None, base_url="https://api.stlouisfed.org", api_key="test_key"):
    return FredAPIClient(
        session=session or mock.Mock(),
        base_url=base_url,
        api_key=api_key,
    )


def test_instantiate_with_correct_attributes():
    mock_session = mock.Mock()
    client = _make_client(session=mock_session, base_url="mock_url.com", api_key="mock_api_key")

    assert client._FredAPIClient__session == mock_session
    assert client._FredAPIClient__base_url == "mock_url.com"
    assert client._FredAPIClient__api_key == "mock_api_key"

        
def test_get_interest_rates_returns_response_json():
    expected = {"observations": [{"date": "2024-01-01", "value": "5.33"}]}
    mock_response = FakeResponse(data=expected)

    mock_session = mock.Mock()
    mock_session.get.return_value = mock_response

    client = _make_client(session=mock_session)
    result = client.get_interest_rates()

    assert result == expected

def test_get_interest_rates_calls_correct_endpoint_and_params():
    mock_session = mock.Mock()
    mock_session.get.return_value = mock.Mock()

    base_url = "https://api.stlouisfed.org"
    api_key = "my_api_key"
    client = _make_client(session=mock_session, base_url=base_url, api_key=api_key)
    client.get_interest_rates()

    mock_session.get.assert_called_once_with(
        url=f"{base_url}/fred/series/observations",
        params={
            "series_id": "FEDFUNDS",
            "file_type": "json",
            "api_key": api_key,
        },
    )

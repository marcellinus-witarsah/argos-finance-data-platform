from pipelines.bronze.api_to_bronze import DataPipeline


def test_data_pipeline_object_attributes_has_expected_value_after_init():
    mock_api_vendor="mock_api_vendor"
    data_pipeline = DataPipeline(api_vendor=mock_api_vendor)
    assert data_pipeline.api_vendor == mock_api_vendor

def test_data_pipeline_get_api_vendor_information_and_credentials_method(monkeypatch):
    mock_api_vendor="mock_api_vendor"

    monkeypatch.setenv("MOCK_API_VENDOR_BASE_URL", "https://www.mockapivendor.com")
    monkeypatch.setenv("MOCK_API_VENDOR_API_KEY", "mock-api-key")

    data_pipeline = DataPipeline(api_vendor=mock_api_vendor)
    data_pipeline.get_api_vendor_information_and_credentials()

    assert data_pipeline._DataPipeline__base_url == "https://www.mockapivendor.com"
    assert data_pipeline._DataPipeline__api_key == "mock-api-key"

def test_returns_none_when_env_vars_are_missing(monkeypatch):
    mock_api_vendor="mock_api_vendor"

    monkeypatch.delenv("MOCK_API_VENDOR_BASE_URL", raising=False)
    monkeypatch.delenv("MOCK_API_VENDOR_API_KEY", raising=False)

    data_pipeline = DataPipeline(api_vendor=mock_api_vendor)
    data_pipeline.get_api_vendor_information_and_credentials()

    assert data_pipeline._DataPipeline__base_url is None
    assert data_pipeline._DataPipeline__api_key is None
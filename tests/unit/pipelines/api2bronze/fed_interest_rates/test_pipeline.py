import pytest

from pipelines.api2bronze.fed_interest_rates.pipeline import get_configuration


class TestPipeline:
    def test_get_configuration_return_correct_results(self):
        cfg = {"extractor": {"query_params": {}}}
        env = {"FRED_API_KEY": "api_key"}

        cfg = get_configuration(cfg=cfg, env=env)

        expected_cfg = {
            "extractor": {
                "query_params": {
                    "api_key": "api_key",
                }
            }
        }

        assert cfg["extractor"] == expected_cfg["extractor"]

    def test_get_configuration_raises_value_error(self):
        cfg = {"extractor": {"query_params": {}}}
        env = {}

        with pytest.raises(ValueError):
            cfg = get_configuration(cfg=cfg, env=env)

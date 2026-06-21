import argparse

import pytest

from pipelines.api2bronze.alpha_vantage_crypto_ohlcv.pipeline import (
    get_configuration, get_parameters)


class TestPipeline:
    def test_get_parameters_via_arguments_return_correct_results(self):
        argv = ["--symbol", "BTC", "--market", "USD"]

        parameters = get_parameters(argv=argv)

        assert parameters.symbol == "BTC"
        assert parameters.market == "USD"

    def test_get_parameters_via_cli_return_correct_results(self, monkeypatch):
        # Simulate cli input
        monkeypatch.setattr(
            "sys.argv",
            ["pipeline.py", "--symbol", "BTC", "--market", "USD"],
        )

        parameters = get_parameters()

        assert parameters.symbol == "BTC"
        assert parameters.market == "USD"

    def test_get_configuration_return_correct_results(self):
        parameters = argparse.Namespace(market="USD", symbol="BTC")
        cfg = {"extractor": {"query_params": {}}}
        env = {"ALPHA_VANTAGE_API_KEY": "api_key"}

        cfg = get_configuration(parameters=parameters, cfg=cfg, env=env)

        expected_cfg = {
            "extractor": {
                "query_params": {
                    "market": "USD",
                    "symbol": "BTC",
                    "apikey": "api_key",
                }
            }
        }

        assert cfg["extractor"] == expected_cfg["extractor"]

    def test_get_configuration_raises_value_error(self):
        parameters = argparse.Namespace(market="USD", symbol="BTC")
        cfg = {"extractor": {"query_params": {}}}
        env = {}

        with pytest.raises(ValueError):
            cfg = get_configuration(parameters=parameters, cfg=cfg, env=env)

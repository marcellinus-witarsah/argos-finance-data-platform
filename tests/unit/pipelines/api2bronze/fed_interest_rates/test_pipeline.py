import pytest

from pipelines.api2bronze.fed_interest_rates.pipeline import (
    get_configuration,
    get_parameters,
)


class TestPipeline:
    def test_get_parameters_via_arguments_return_correct_results(self):
        argv = ["--conf-yaml-file", "./configs/api2bronze/fed_interest_rates.yaml"]

        parameters = get_parameters(argv=argv)

        assert (
            parameters.conf_yaml_file == "./configs/api2bronze/fed_interest_rates.yaml"
        )

    def test_get_parameters_via_cli_return_correct_results(self, monkeypatch):
        # Simulate cli input
        monkeypatch.setattr(
            "sys.argv",
            [
                "pipeline.py",
                "--conf-yaml-file",
                "./configs/api2bronze/fed_interest_rates.yaml",
            ],
        )

        parameters = get_parameters()

        assert (
            parameters.conf_yaml_file == "./configs/api2bronze/fed_interest_rates.yaml"
        )

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

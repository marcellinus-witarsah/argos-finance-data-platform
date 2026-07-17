import os

import pytest
import yaml

from src.strategy.parser.yaml_parser_strategy import YAMLParserStrategy


class TestYamlParseStrategy:
    def test_if_raises_file_not_found_exception(self):
        mock_invalid_filepath = "./mock_invalid_filepath.yaml"

        yaml_parser_strategy = YAMLParserStrategy()

        with pytest.raises(FileNotFoundError):
            yaml_parser_strategy.parse(mock_invalid_filepath)

    def test_if_yaml_filepath_and_content_valid_return_a_dictionary(self):
        config_data = {"mock_key_1": "mock_value_1", "mock_key_2": "mock_value_2"}

        with open("./mock_valid_filetpath.yaml", "w") as file:
            yaml.safe_dump(config_data, file, default_flow_style=False, sort_keys=False)

        yaml_parser_strategy = YAMLParserStrategy()
        result = yaml_parser_strategy.parse("./mock_valid_filetpath.yaml")

        os.remove("./mock_valid_filetpath.yaml")

        assert type(result) is dict
        assert result["mock_key_1"] == "mock_value_1"
        assert result["mock_key_2"] == "mock_value_2"

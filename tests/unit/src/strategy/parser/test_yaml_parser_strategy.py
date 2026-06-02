import yaml
import os
from src.strategy.parser.yaml_parser_strategy import YAMLParserStrategy
from unittest import mock
import unittest


class TestYamlParseStrategy(unittest.TestCase):
    def __make_yaml_parser_strategy(self):
        mock_logger = mock.Mock()
        return YAMLParserStrategy(mock_logger)

    def test_if_raises_file_not_found_exception(self):
        mock_invalid_filepath = "./mock_invalid_filepath.yaml"

        yaml_parser_strategy = self.__make_yaml_parser_strategy()

        with self.assertRaises(FileNotFoundError):
            yaml_parser_strategy.parse(mock_invalid_filepath)

    def test_if_yaml_filepath_and_content_valid_return_a_dictionary(self):
        config_data = {"mock_key_1": "mock_value_1", "mock_key_2": "mock_value_2"}

        with open("./mock_valid_filetpath.yaml", "w") as file:
            yaml.safe_dump(config_data, file, default_flow_style=False, sort_keys=False)

        yaml_parser_strategy = self.__make_yaml_parser_strategy()
        result = yaml_parser_strategy.parse("./mock_valid_filetpath.yaml")

        os.remove("./mock_valid_filetpath.yaml")

        assert type(result) == dict
        assert result["mock_key_1"] == "mock_value_1"
        assert result["mock_key_2"] == "mock_value_2"

from src.strategy.hasher.md5_hasher_strategy import MD5HasherStrategy
from unittest import mock
import unittest


class TestMD5HasherStrategy(unittest.TestCase):
    def __make_md5_hasher_strategy(self):
        mock_logger = mock.Mock()
        return MD5HasherStrategy()

    def test_if_raises_attribute_error_exception(self):
        mock_invalid_type = 12345

        md5_hasher_strategy = self.__make_md5_hasher_strategy()

        with self.assertRaises(AttributeError):
            md5_hasher_strategy.hash(mock_invalid_type)

    def test_if_valid_string_returns_correct_md5_hash(self):
        mock_data = "mock_data"
        expected = "d7ce6c5a6bbf10ea8198d006da29eaf9"

        md5_hasher_strategy = self.__make_md5_hasher_strategy()
        result = md5_hasher_strategy.hash(mock_data)

        assert result == expected

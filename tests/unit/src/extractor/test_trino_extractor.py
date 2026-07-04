import pytest

from src.extractor.trino_extractor import TrinoExtractor


class TestTrinoExtractor:
    def test_extract_returns_cursor_rows_on_success(self, mocker):
        mock_conn = mocker.Mock()
        mock_cursor = mock_conn.cursor.return_value
        mock_cursor.fetchall.return_value = [("BTC", 65000.0), ("ETH", 2000.0)]

        rows = TrinoExtractor(mock_conn).extract("bronze.prices", ["symbol", "price"])

        assert rows == [("BTC", 65000.0), ("ETH", 2000.0)]
        mock_cursor.fetchall.assert_called_once_with()

    def test_extract_builds_expected_sql(self, mocker):
        mock_conn = mocker.Mock()
        mock_cursor = mock_conn.cursor.return_value

        TrinoExtractor(mock_conn).extract("bronze.prices", ["symbol", "price"])

        mock_cursor.execute.assert_called_once()
        sql = " ".join(mock_cursor.execute.call_args.args[0].split())
        assert "SELECT symbol,price" in sql
        assert "FROM bronze.prices" in sql

    def test_extract_raises_exception_on_failed(self, mocker):
        mock_conn = mocker.Mock()
        mock_cursor = mock_conn.cursor.return_value
        mock_cursor.execute.side_effect = Exception("Query failed")

        with pytest.raises(Exception, match="Query failed"):
            TrinoExtractor(mock_conn).extract("bronze.prices", ["symbol", "price"])

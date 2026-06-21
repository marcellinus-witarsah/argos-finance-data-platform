import json
import unittest
from unittest.mock import Mock, patch

from pyspark.sql.types import DateType, StringType, TimestampType

from pipelines.api2bronze.alpha_vantage_crypto_ohlcv.pipeline import run
from tests.integration.conftest import Conftest

SAMPLE_RESPONSE = {
    "Meta Data": {
        "1. Information": "Daily Prices and Volumes for Digital Currency",
        "2. Digital Currency Code": "BTC",
        "3. Digital Currency Name": "Bitcoin",
        "4. Market Code": "USD",
        "5. Last Refreshed": "2024-01-02",
        "6. Time Zone": "UTC",
    },
    "Time Series (Digital Currency Daily)": {
        "2024-01-02": {
            "1a. open (USD)": "42000.00",
            "2a. high (USD)": "43500.00",
            "3a. low (USD)": "41800.00",
            "4a. close (USD)": "43200.00",
            "5. volume": "15234.56",
            "6. market cap (USD)": "15234.56",
        }
    },
}

SAMPLE_CFG = {
    "extractor": {
        "url": "https://www.alphavantage.co/query",
        "query_params": {
            "function": "DIGITAL_CURRENCY_DAILY",
            "symbol": "BTC",
            "market": "USD",
            "apikey": "test_key",
        },
        "headers": {},
    },
    "writer": {
        "table": "catalog.argos_finance_catalog.bronze.alpha_vantage_crypto_ohlcv",
        "fmt": "iceberg",
        "mode": "merge",
        "merge_columns": ["id"],
        "partition_columns": [],
    },
}


class TestAlphaVantageCryptoOHLCVPipeline(Conftest):
    def _run_with_mocks(self):
        mock_response = Mock()
        mock_response.json.return_value = SAMPLE_RESPONSE
        mock_response.raise_for_status.return_value = None

        with patch("requests.Session.get", return_value=mock_response), patch(
            "src.writer.spark_dataframe_writer.SparkDataframeWriter.write"
        ) as mock_write:
            run(cfg=SAMPLE_CFG)

        return mock_write

    def test_run_produces_valid_schema(self):
        mock_write = self._run_with_mocks()

        mock_write.assert_called_once()
        df = mock_write.call_args.kwargs["df"]

        assert "json_data" in df.columns
        assert "id" in df.columns
        assert "load_dttm" in df.columns
        assert "load_prdt" in df.columns

    def test_run_produces_single_row(self):
        mock_write = self._run_with_mocks()

        df = mock_write.call_args.kwargs["df"]
        assert df.count() == 1

    def test_run_stores_raw_json(self):
        mock_write = self._run_with_mocks()

        df = mock_write.call_args.kwargs["df"]
        row = df.first()
        assert json.loads(row["json_data"]) == SAMPLE_RESPONSE

    def test_run_generates_nonnull_id(self):
        mock_write = self._run_with_mocks()

        df = mock_write.call_args.kwargs["df"]
        row = df.first()
        assert row["id"] is not None
        assert len(row["id"]) == 32

    def test_run_column_types(self):
        mock_write = self._run_with_mocks()

        df = mock_write.call_args.kwargs["df"]
        schema = {field.name: field.dataType for field in df.schema.fields}

        assert isinstance(schema["json_data"], StringType)
        assert isinstance(schema["id"], StringType)
        assert isinstance(schema["load_dttm"], TimestampType)
        assert isinstance(schema["load_prdt"], DateType)

    def test_run_passes_correct_writer_args(self):
        mock_write = self._run_with_mocks()

        call_kwargs = mock_write.call_args.kwargs
        assert call_kwargs["fmt"] == "iceberg"
        assert call_kwargs["mode"] == "merge"
        assert call_kwargs["table"] == SAMPLE_CFG["writer"]["table"]
        assert call_kwargs["merge_columns"] == ["id"]


if __name__ == "__main__":
    unittest.main()

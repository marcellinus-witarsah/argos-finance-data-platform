import json
import unittest
from unittest.mock import Mock, patch

from pyspark.sql.types import DateType, StringType, TimestampType

from pipelines.api2bronze.fed_interest_rates.pipeline import run
from tests.integration.conftest import Conftest

SAMPLE_RESPONSE = {
    "realtime_start": "2024-01-01",
    "realtime_end": "2024-01-01",
    "observation_start": "1954-07-01",
    "observation_end": "9999-12-31",
    "units": "lin",
    "output_type": 1,
    "file_type": "json",
    "order_by": "observation_date",
    "sort_order": "asc",
    "count": 2,
    "offset": 0,
    "limit": 100000,
    "observations": [
        {"realtime_start": "2024-01-01", "realtime_end": "2024-01-01", "date": "2023-11-01", "value": "5.33"},
        {"realtime_start": "2024-01-01", "realtime_end": "2024-01-01", "date": "2023-12-01", "value": "5.33"},
    ],
}

SAMPLE_CFG = {
    "extractor": {
        "url": "https://api.stlouisfed.org/fred/series/observations",
        "query_params": {
            "series_id": "FEDFUNDS",
            "file_type": "json",
            "api_key": "test_key",
        },
        "headers": {},
    },
    "writer": {
        "table": "catalog.argos_finance_catalog.bronze.fed_interest_rates",
        "fmt": "iceberg",
        "mode": "merge",
        "merge_columns": ["id"],
        "partition_columns": [],
    },
}


class TestFedInterestRatesPipeline(Conftest):
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

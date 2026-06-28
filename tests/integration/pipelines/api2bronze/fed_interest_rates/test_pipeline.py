import datetime
import hashlib
import json

import chispa
import pyspark.sql.functions as F
from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipelines.api2bronze.fed_interest_rates.pipeline import run

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
        {
            "realtime_start": "2024-01-01",
            "realtime_end": "2024-01-01",
            "date": "2023-11-01",
            "value": "5.33",
        },
        {
            "realtime_start": "2024-01-01",
            "realtime_end": "2024-01-01",
            "date": "2023-12-01",
            "value": "5.33",
        },
    ],
}

SAMPLE_CFG = {
    "extractor": {
        "url": "https://api.stlouisfed.org/fred/series/observations",
        "query_params": {
            "series_id": "FEDFUNDS",
            "file_type": "json",
            "api_key": "FRED_API_KEY",
        },
        "headers": None,
    },
    "transformer": None,
    "writer": {
        "table": "argos_finance_catalog.bronze.fed_interest_rates",
        "fmt": "iceberg",
        "mode": "merge",
        "merge_columns": ["id"],
    },
}


class TestFEDInterestRatesPipeline:
    def test_run_writes_to_spark_table(self, spark, mocker):
        mock_response = mocker.Mock()
        mock_response.json.return_value = SAMPLE_RESPONSE
        mock_response.raise_for_status.return_value = None
        mocker.patch("requests.Session.get", return_value=mock_response)

        mocker.patch("src.utils.spark_session.get_spark", new=spark)

        FIXED_TIMESTAMP = datetime.datetime(2026, 1, 1, 10, 0, 0)
        FIXED_DATE = datetime.date(2026, 1, 1)

        mocker.patch(
            "pipelines.shared.transform.F.current_timestamp",
            return_value=F.lit(FIXED_TIMESTAMP),
        )
        mocker.patch(
            "pipelines.shared.transform.F.to_date", return_value=F.lit(FIXED_DATE)
        )

        # Expected Dataframe
        expected_df = spark.createDataFrame(
            [
                (
                    json.dumps(SAMPLE_RESPONSE),
                    hashlib.md5(
                        json.dumps(SAMPLE_RESPONSE).encode("utf-8")
                    ).hexdigest(),
                    FIXED_TIMESTAMP,
                    FIXED_DATE,
                ),
            ],
            schema=StructType(
                [
                    StructField("json_data", StringType(), True),
                    StructField("id", StringType(), True),
                    StructField("load_dttm", TimestampType(), True),
                    StructField("load_prdt", DateType(), True),
                ]
            ),
        )
        table_name = "argos_finance_catalog.bronze.fed_interest_rates"
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        run(cfg=SAMPLE_CFG)
        df = spark.read.table(table_name)

        assert df.count() == 1
        chispa.dataframe_comparer.assert_schema_equality(
            expected_df.schema, df.schema, ignore_nullable=True
        )

        chispa.dataframe_comparer.assert_df_equality(
            expected_df, df, ignore_nullable=True, ignore_row_order=True
        )

        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

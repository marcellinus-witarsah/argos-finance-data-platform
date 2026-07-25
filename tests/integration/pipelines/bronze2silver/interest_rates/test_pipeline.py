import argparse
import datetime
import json

import chispa
import pyspark.sql.functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipelines.bronze2silver.interest_rates.pipeline import main

json_data = {
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
            "value": "4.33",
        },
        {
            "realtime_start": "2024-01-01",
            "realtime_end": "2024-01-01",
            "date": "2023-12-01",
            "value": "5.33",
        },
    ],
}


class TestInterestRatesPipeline:
    def test_run_writes_to_spark_table(self, spark, mocker):
        source_table_name = "argos_finance_catalog.bronze.fed_interest_rates"

        mocker.patch(
            "pipelines.bronze2silver.interest_rates.pipeline.get_parameters",
            return_value=argparse.Namespace(
                conf_yaml_file="./configs/bronze2silver/interest_rates.yaml"
            ),
        )

        FIXED_TIMESTAMP = datetime.datetime(2024, 1, 2, 7, 0, 0)
        FIXED_DATE = datetime.date(2024, 1, 2)
        source_df = spark.createDataFrame(
            data=[
                (json.dumps(json_data), "1", FIXED_TIMESTAMP, FIXED_DATE),
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
        spark.sql(f"DROP TABLE IF EXISTS {source_table_name}")
        source_df.writeTo(source_table_name).using("iceberg").create()

        mocker.patch(
            "pipelines.shared.transform.F.current_timestamp",
            return_value=F.lit(FIXED_TIMESTAMP),
        )
        mocker.patch(
            "pipelines.shared.transform.F.to_date", return_value=F.lit(FIXED_DATE)
        )

        mocker.patch("src.utils.spark_session.get_spark", new=spark)

        expected_df = spark.createDataFrame(
            data=[
                (datetime.date(2023, 11, 1), 4.33, FIXED_TIMESTAMP, FIXED_DATE),
                (datetime.date(2023, 12, 1), 5.33, FIXED_TIMESTAMP, FIXED_DATE),
            ],
            schema=StructType(
                [
                    StructField("date", DateType(), True),
                    StructField("rate", DoubleType(), True),
                    StructField("load_dttm", TimestampType(), True),
                    StructField("load_prdt", DateType(), True),
                ]
            ),
        )
        table_name = "argos_finance_catalog.silver.interest_rates"
        main()
        df = spark.read.table(table_name)

        df.show()

        assert df.count() == 2

        chispa.dataframe_comparer.assert_schema_equality(
            expected_df.schema, df.schema, ignore_nullable=True
        )

        chispa.dataframe_comparer.assert_df_equality(
            expected_df, df, ignore_nullable=True, ignore_row_order=True
        )

        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.sql(f"DROP TABLE IF EXISTS {source_table_name}")

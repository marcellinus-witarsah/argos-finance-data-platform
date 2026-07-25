import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG


SPARK_APPS_DIR = "/opt/spark/apps"
SPARK_CONN_ID = "spark_default"

with DAG(
    dag_id="bitcoin_ohlcv_vs_fed_interest_rates_dag",
    start_date=datetime.datetime(2026, 7, 1),
    schedule="@daily",
):
    # Bronze layer
    alpha_vantage_crypto_ohlcv = SparkSubmitOperator(
        task_id="alpha_vantage_crypto_ohlcv",
        conn_id=SPARK_CONN_ID,
        application=f"{SPARK_APPS_DIR}/pipelines/api2bronze/alpha_vantage_crypto_ohlcv/pipeline.py",
        application_args=[
            "--symbol",
            "BTC",
            "--market",
            "USD",
            "--conf-yaml-file",
            f"{SPARK_APPS_DIR}/configs/api2bronze/alpha_vantage_crypto_ohlcv.yaml",
        ],
        name="alpha_vantage_crypto_ohlcv",
    )

    fed_interest_rates = SparkSubmitOperator(
        task_id="fed_interest_rates",
        conn_id=SPARK_CONN_ID,
        application=f"{SPARK_APPS_DIR}/pipelines/api2bronze/fed_interest_rates/pipeline.py",
        application_args=[
            "--conf-yaml-file",
            f"{SPARK_APPS_DIR}/configs/api2bronze/fed_interest_rates.yaml",
        ],
        name="fed_interest_rates",
    )

    # Silver layer
    crypto_ohlcv = SparkSubmitOperator(
        task_id="crypto_ohlcv",
        conn_id=SPARK_CONN_ID,
        application=f"{SPARK_APPS_DIR}/pipelines/bronze2silver/crypto_ohclv/pipeline.py",
        application_args=[
            "--conf-yaml-file",
            f"{SPARK_APPS_DIR}/configs/bronze2silver/crypto_ohlcv.yaml",
        ],
        name="crypto_ohlcv",
    )

    interest_rates = SparkSubmitOperator(
        task_id="interest_rates",
        conn_id=SPARK_CONN_ID,
        application=f"{SPARK_APPS_DIR}/pipelines/bronze2silver/interest_rates/pipeline.py",
        application_args=[
            "--conf-yaml-file",
            f"{SPARK_APPS_DIR}/configs/bronze2silver/interest_rates.yaml",
        ],
        name="interest_rates",
    )

    # Gold layer
    bitcoin_ohlcv_vs_interest_rates = SparkSubmitOperator(
        task_id="bitcoin_ohlcv_vs_interest_rates",
        conn_id=SPARK_CONN_ID,
        application=f"{SPARK_APPS_DIR}/pipelines/silver2gold/bitcoin_ohlcv_vs_fed_interest_rates/pipeline.py",
        application_args=[
            "--conf-yaml-file",
            f"{SPARK_APPS_DIR}/configs/silver2gold/bitcoin_ohlcv_vs_fed_interest_rates.yaml",
        ],
        name="bitcoin_ohlcv_vs_interest_rates",
    )

    alpha_vantage_crypto_ohlcv >> crypto_ohlcv
    fed_interest_rates >> interest_rates
    [crypto_ohlcv, interest_rates] >> bitcoin_ohlcv_vs_interest_rates

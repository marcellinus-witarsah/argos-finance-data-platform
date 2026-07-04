import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots
from trino.dbapi import connect

from src.extractor.trino_extractor import TrinoExtractor
from src.utils.spark_session import get_spark

# Establish the connection
trino_extractor = TrinoExtractor(
    conn=connect(
        host="trino",
        port=8080,
        user="admin",
        catalog="argos_finance_catalog",
        schema="default",
    )
)
table = "argos_finance_catalog.gold.bitcoin_ohlcv_vs_fed_interest_rates"
columns = [
    "ticker",
    "currency",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "rate",
]
rows = trino_extractor.extract(table=table, columns=columns)
data = pd.DataFrame(data=rows, columns=columns)

# Load gold data and cache
chart = st.empty()

if not data.empty:
    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Candlestick(
            x=data.index,
            open=data["open"],
            high=data["high"],
            low=data["low"],
            close=data["close"],
            increasing_line_color="green",
            decreasing_line_color="red",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=data.index,
            y=data["rate"],
            mode="lines",
            name="lines",
        ),
        secondary_y=True,
    )

    # Customize layout
    fig.update_layout(
        title=f"{data['ticker'].unique().tolist()[0]} Candlestick Chart",
        yaxis_title=f"Price ({data['currency'].unique().tolist()[0]})",
        xaxis_rangeslider_visible=False,
        height=600,
    )

    # Render Plot in Streamlit
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No data found. Please check the ticker symbol.")

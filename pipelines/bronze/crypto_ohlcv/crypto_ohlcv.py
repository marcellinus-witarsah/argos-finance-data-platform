from ingestion.api.base_api import BaseAPI
from src.ingestion.api.alpha_vantage_api import AlphaVantageAPI
from dotenv import load_dotenv, find_dotenv
import os

def fetch_crypto_ohlcv(ingestion: BaseAPI):
    return ingestion.fetch_ohlcv(params={"symbol": "BTC", "market": "USD"})

def main(ingestion: BaseAPI):
    # Fetch data from an API
    response = fetch_crypto_ohlcv(ingestion)




if __name__ == "__main__":
    load_dotenv(find_dotenv())

    # if ingestion == "alpha_vantage":
    #     ingestion = AlphaVantageAPI(api_key=os.get_env("ALPHA_VANTAGE_API_KEY"))
    # else:
    #     raise ValueError(f"Unsupported ingestion API: {ingestion_api}") 

    # if repository
    ingestion = AlphaVantageAPI(api_key=os.getenv("ALPHA_VANTAGE_API_KEY"))
    main(ingestion=ingestion)
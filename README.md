# argos-finance-data-platform
A market data platform for financial data

# Documentation
## 12 April 2026
The architecture pattern that I use is Clean Architecture which uses abstraction for my pipelines to interact with external pipelines. I designed this way because: 
1. I don't want to be stuck modifying multiple pipelines just because of a change in API.
2. This also allows me to change APIs freely without modifying the pipelines.

Pipeline decisions:
1. Using data as the primary because data from the past OHLCV will never be changed so using the as the or using a hash code for the id 

## 26 April 2026
For the API, We don't need an abstraction and then create a new class based on API provider that we get our data because all API(s) is performed the same way. The key differences will be the base URL, endpoint, query params, headers, body, and api key. 

Using the API config with the model is not a good idea because query params, headers, body and api key might be vary for different api.

## 21 May 2026
Decided to created an abstract for each extractor, writer. while transformer will be defined inside each pipelien because each pipeline varies of how the transformation will be done.

## Road Map
### Phase 1
- [x] Ingestion (Alpha Vantage)
- [x] Store in Iceberg
- [x] Execute using Spark
- [x] Manage metadata using Apache Gravitino and use Postgres as metastore backend
- [ ] Apache Superset for Displaying data

### Phase 2 (SOON)
Basic API (/btc/price)
### Phase 3 (SOON)
Indicators + /btc/signal
### Phase 4 (SOON)
Clean architecture + caching
### Phase 5 (SOON)
AI agent + public API

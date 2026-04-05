import json
from abc import ABC, abstractmethod


class BaseAPI(ABC):
    @abstractmethod
    def fetch_ohlcv(self, params: dict) -> json:
        raise NotImplementedError("fetch_ohlcv method must be implemented by subclass")

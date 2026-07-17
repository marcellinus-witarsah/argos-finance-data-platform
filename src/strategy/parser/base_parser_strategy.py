from abc import ABC, abstractmethod
import logging


class BaseParserStrategy(ABC):
    @abstractmethod
    def parse(self, filepath: str) -> dict:
        pass

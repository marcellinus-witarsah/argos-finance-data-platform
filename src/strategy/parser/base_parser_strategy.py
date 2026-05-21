from abc import ABC, abstractmethod

class BaseParserStrategy(ABC):
    @abstractmethod
    def parse(self, filepath: str) -> dict:
        pass

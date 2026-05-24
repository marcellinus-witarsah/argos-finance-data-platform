from abc import ABC, abstractmethod
import logging

class BaseParserStrategy(ABC):
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        
    @abstractmethod
    def parse(self, filepath: str) -> dict:
        pass

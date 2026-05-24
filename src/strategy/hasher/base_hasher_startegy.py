from abc import ABC, abstractmethod
import logging


class BaseHasherStrategy(ABC):
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    @abstractmethod
    def hash(data: str):
        pass

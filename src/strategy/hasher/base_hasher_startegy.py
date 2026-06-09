from abc import ABC, abstractmethod

class BaseHasherStrategy(ABC):
    @abstractmethod
    def hash(data: str):
        pass

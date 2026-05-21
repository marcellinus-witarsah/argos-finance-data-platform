from src.strategy.hasher.base_hasher_startegy import BaseHasherStrategy

class HasherContext:
    def __init__(self, strategy: BaseHasherStrategy):
        self._strategy = strategy

    def set_strategy(self, strategy: BaseHasherStrategy):
        self._strategy = strategy

    def hash(self, filepath: str) -> dict:
        return self._strategy.hash(filepath)

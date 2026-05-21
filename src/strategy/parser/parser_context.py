from src.strategy.parser.base_parser_strategy import BaseParserStrategy

class ParserContext:
    def __init__(self, strategy: BaseParserStrategy):
        self._strategy = strategy

    def set_strategy(self, strategy: BaseParserStrategy):
        self._strategy = strategy

    def parse(self, filepath: str) -> dict:
        return self._strategy.parse(filepath)

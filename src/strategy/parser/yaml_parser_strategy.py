import yaml
from src.strategy.parser.base_parser_strategy import BaseParserStrategy

class YAMLParserStrategy(BaseParserStrategy):
    def parse(self, filepath: str) -> dict:
        result = None
        # try:
        with open(filepath, "r") as f:
            result = yaml.safe_load(f)
        #     logger.info(f"{filepath} parsed successfully.")
        # except Exception as e:
        #     logger.error(e)
        return result

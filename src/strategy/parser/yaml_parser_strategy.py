import yaml
from src.strategy.parser.base_parser_strategy import BaseParserStrategy

class YAMLParserStrategy(BaseParserStrategy):
    def parse(self, filepath: str) -> dict:
        try:
            with open(filepath, "r") as f:
                result = yaml.safe_load(f)
                self.logger.info(f"{filepath} parsed successfully.")
                return result
        except Exception as e:
            self.logger.error(e)
            raise e

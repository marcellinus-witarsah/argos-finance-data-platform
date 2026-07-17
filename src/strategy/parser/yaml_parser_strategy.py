import yaml
from src.strategy.parser.base_parser_strategy import BaseParserStrategy
from src.utils.logger import logger

class YAMLParserStrategy(BaseParserStrategy):
    def parse(self, filepath: str) -> dict:
        try:
            with open(filepath, "r") as f:
                result = yaml.safe_load(f)
                logger.info(f"{filepath} parsed successfully.")
                return result
        except Exception as e:
            logger.error(e)
            raise e

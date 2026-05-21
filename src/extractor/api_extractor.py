from src.extractor.base_extractor import BaseExtractor

class APIExtractor(BaseExtractor):
    def extract(self) -> dict:
        response = self.ctx.session.get(
            url=self.cfg["url"],
            params=self.cfg["query_params"],
            headers=self.cfg["headers"]
        )
        return response.json()

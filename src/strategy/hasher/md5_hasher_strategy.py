import hashlib
from src.strategy.hasher.base_hasher_startegy import BaseHasherStrategy


class MD5HasherStrategy(BaseHasherStrategy):
    def hash(self, data: str):
        try:
            hshlb = hashlib.md5()
            hshlb.update(data.encode('utf-8'))
            result = hshlb.hexdigest()
            self.logger.info("Generated hash using MD5 Algorithm")
            return result
        except Exception as e:
            self.logger.error(e)
            raise e

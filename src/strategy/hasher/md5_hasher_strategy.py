import hashlib
from src.strategy.hasher.base_hasher_startegy import BaseHasherStrategy

class MD5HasherStrategy(BaseHasherStrategy):
    def hash(self, data: str):
        result = None
        # try:
        hshlb = hashlib.md5()
        hshlb.update(data.encode())
        result = hshlb.hexdigest()
        #     self.__logger.info("Generated hash using MD5 Algorithm")
        # except Exception as e:
        #     self.__logger.error(e)
        return result

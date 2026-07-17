# ================================================================================
# Author: Marcellinus A.W.
# Date: 16 May 2026
# Description: Decided to create an abstract class for extractor so that each new
# extractor has to inherite from this base class
# ================================================================================

from abc import ABC, abstractmethod


class BaseWriter(ABC):
    @abstractmethod
    def write(self):
        raise NotImplementedError("Please implement this method")

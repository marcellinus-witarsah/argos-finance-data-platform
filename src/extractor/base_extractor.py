# ================================================================================
# Author: Marcellinus A.W.
# Date: 16 May 2026
# Description: Decided to create an abstract class for extractor so that each new
# extractor has to inherite from this base class
# ================================================================================

from abc import ABC, abstractmethod
from src.utils.context import PipelineContext


class BaseExtractor(ABC):
    def __init__(self, ctx: PipelineContext, cfg: dict):
        self.ctx = ctx
        self.cfg = cfg

    @abstractmethod
    def extract(self):
        raise NotImplementedError("Please implement this method")

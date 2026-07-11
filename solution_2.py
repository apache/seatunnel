from typing import Optional

class TiDBSourceReaderConfig:
    def __init__(self, resolvedTs: Optional[int] = None, lastKnownGoodTs: Optional[int] = None):
        self.resolvedTs = resolvedTs
        self.lastKnownGoodTs = lastKnownGoodTs
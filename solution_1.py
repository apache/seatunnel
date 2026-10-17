from typing import Optional
import logging

class CDCClient:
    def __init__(self, config: 'CDCClientConfig'):
        self.config = config
        self.resolvedTs = 0
        self.lastKnownGoodTs = 0
        self.logger = logging.getLogger(__name__)

    def handleResolvedTs(self, resolvedTs: int, regionId: str) -> None:
        self.resolvedTs = resolvedTs
        self.lastKnownGoodTs = self.resolvedTs
        self.logger.info(f"handle resolvedTs: {resolvedTs}, regionId: {regionId}")

    def checkResolvedTs(self) -> None:
        if self.resolvedTs - self.lastKnownGoodTs > 1000:  # 1 second
            self.resolvedTs = self.lastKnownGoodTs
            self.logger.warning(f"Reset resolvedTs to last known good value: {self.lastKnownGoodTs}")

    def getResolvedTs(self) -> Optional[int]:
        self.checkResolvedTs()
        return self.resolvedTs
from typing import Optional
import logging

class TiDBSourceReader:
    def __init__(self, config: 'TiDBSourceReaderConfig'):
        self.config = config
        self.resolvedTs = 0
        self.lastKnownGoodTs = 0
        self.logger = logging.getLogger(__name__)

    def captureStreamingEvent(self, event: dict) -> None:
        if event['ts'] > self.resolvedTs:
            self.resolvedTs = event['ts']
            self.lastKnownGoodTs = self.resolvedTs
            self.logger.info(f"Captured streaming event from resolvedTs: {self.resolvedTs}")
        elif event['ts'] == self.resolvedTs:
            self.logger.info(f"Duplicate streaming event from resolvedTs: {self.resolvedTs}")

    def checkResolvedTs(self) -> None:
        if self.resolvedTs - self.lastKnownGoodTs > 1000:  # 1 second
            self.resolvedTs = self.lastKnownGoodTs
            self.logger.warning(f"Reset resolvedTs to last known good value: {self.lastKnownGoodTs}")

    def getResolvedTs(self) -> Optional[int]:
        self.checkResolvedTs()
        return self.resolvedTs
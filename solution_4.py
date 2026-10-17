class TiDBSourceReader:
    def __init__(self, config: TiDBSourceReaderConfig):
        self.config = config
        self.resolvedTs = config.resolvedTs
        self.lastKnownGoodTs = config.lastKnownGoodTs
        self.logger = logging.getLogger(__name__)

    # ...
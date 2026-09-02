class CDCClient:
    def __init__(self, config: CDCClientConfig):
        self.config = config
        self.resolvedTs = config.resolvedTs
        self.lastKnownGoodTs = config.lastKnownGoodTs
        self.logger = logging.getLogger(__name__)

    # ...
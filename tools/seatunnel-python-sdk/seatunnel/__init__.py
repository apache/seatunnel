from .client import Client
from .endpoints.cluster import ClusterApi

class SeaTunnelClient:
    def __init__(self, base_url, token=None):
        self.client = Client(base_url, token)

        self.cluster = ClusterApi(self.client)

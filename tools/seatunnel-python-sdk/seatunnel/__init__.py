from .client import Client
from .endpoints.cluster import ClusterApi
from .endpoints.jobs import JobsApi
from .endpoints.config import ConfigApi

class SeaTunnelClient:
    def __init__(self, base_url, token=None):
        self.client = Client(base_url, token)

        self.cluster = ClusterApi(self.client)
        self.jobs = JobsApi(self.client)
        self.config = ConfigApi(self.client)

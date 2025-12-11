import httpx
from .helpers.httpMethod import HttpMethod
from .endpoints.cluster import ClusterApi
from .endpoints.jobs import JobsApi
from .endpoints.config import ConfigApi
from .endpoints.system import SystemApi

class Client:
    def __init__(self, base_url: str, timeout: float = 10):
        self.base_url = base_url
        self.timeout = timeout
        self.session = httpx.Client(timeout=timeout)

    def request(self, method: HttpMethod, path: str, **kwargs):
        resp = self.session.request(
            method.value,
            f"{self.base_url}{path}",
            **kwargs
        )
        
        content_type = resp.headers.get("Content-Type", "")
        if "application/json" in content_type:
            return resp.json()
        else:
            return resp.text
        
class SeaTunnelClient:
    def __init__(self, base_url):
        self.client = Client(base_url)

        self.cluster = ClusterApi(self.client)
        self.jobs = JobsApi(self.client)
        self.config = ConfigApi(self.client)
        self.system = SystemApi(self.client)
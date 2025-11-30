import httpx
from .helpers.httpMethod import HttpMethod

class Client:
    def __init__(self, base_url: str, token: str | None = None, timeout: float = 10):
        self.base_url = base_url
        self.token = token
        self.timeout = timeout
        self.session = httpx.Client(timeout=timeout)

    def request(self, method: HttpMethod, path: str, **kwargs):
        resp = self.session.request(
            method.value,
            f"{self.base_url}{path}",
            **kwargs
        )
        
        resp.raise_for_status()
        return resp.json()
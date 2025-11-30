import httpx
from .helpers.httpMethod import HttpMethod

class Client:
    def __init__(self, base_url: str, token: str | None = None, timeout: float = 10):
        self.base_url = base_url
        self.token = token
        self.timeout = timeout
        self.session = httpx.Client(timeout=timeout)

    def _headers(self):
        h = {"Content-Type": "application/json"}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        return h

    def request(self, method: HttpMethod, path: str, **kwargs):
        resp = self.session.request(
            method.value,
            f"{self.base_url}{path}",
            headers=self._headers(),
            **kwargs
        )
        
        resp.raise_for_status()
        return resp.json()
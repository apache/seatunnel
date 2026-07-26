#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


import httpx
from typing import Any

from .helpers.httpMethod import HttpMethod


class SeaTunnelError(Exception):
    """Base exception for SeaTunnel SDK"""

    pass


class SeaTunnelAPIError(SeaTunnelError):
    """API error (4xx/5xx responses)"""

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"[{status_code}] {message}")


class SeaTunnelConnectionError(SeaTunnelError):
    """Network connection error"""

    pass


class Client:
    def __init__(self, base_url: str, timeout: float = 10):
        self.base_url = base_url
        self.timeout = timeout
        self.session = httpx.Client(timeout=timeout)

    def request(self, method: HttpMethod, path: str, **kwargs: Any):
        """Send a REST request and normalize transport and API failures."""
        try:
            resp = self.session.request(method.value, f"{self.base_url}{path}", **kwargs)
            resp.raise_for_status()
        except httpx.RequestError as exc:
            raise SeaTunnelConnectionError(f"Failed to connect to {exc.request.url}: {exc}") from exc
        except httpx.HTTPStatusError as exc:
            try:
                error_data = exc.response.json()
                message = error_data.get("message", exc.response.text)
            except ValueError:
                message = exc.response.text

            raise SeaTunnelAPIError(status_code=exc.response.status_code, message=message) from exc

        content_type = resp.headers.get("Content-Type", "")
        return resp.json() if "application/json" in content_type else resp.text

    def close(self):
        self.session.close()


class SeaTunnelClient:
    def __init__(self, base_url):
        from .endpoints.config import ConfigApi
        from .endpoints.cluster import ClusterApi
        from .endpoints.jobs import JobsApi
        from .endpoints.system import SystemApi

        self.client = Client(base_url)

        self.cluster = ClusterApi(self.client)
        self.jobs = JobsApi(self.client)
        self.config = ConfigApi(self.client)
        self.system = SystemApi(self.client)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

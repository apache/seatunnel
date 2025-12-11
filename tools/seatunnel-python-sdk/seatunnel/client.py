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
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


from typing import Mapping, Optional

from ..client import Client
from ..helpers.httpMethod import HttpMethod


class ClusterApi:
    def __init__(self, client: Client):
        self.client = client

    def get_overview(self, params: Optional[Mapping[str, str]] = None):
        """
        Returns an overview over the Zeta engine cluster.
        """
        # The REST API accepts arbitrary tag key/value pairs as raw query params.
        return self.client.request(HttpMethod.GET, "/overview", params=params)

    def get_metrics(self):
        """
        Get Node Metrics
        """
        return self.client.request(HttpMethod.GET, "/openmetrics")

    def get_log(self):
        """
        Get Log Content from a Single Node
        """
        return self.client.request(HttpMethod.GET, "/log")

    def get_logs(self, jobId: Optional[int] = None):
        """
        Get Logs from All Nodes
        """
        # The current REST servlet reads the job filter from `/logs/{jobId}`.
        path = "/logs" if jobId is None else f"/logs/{jobId}"
        return self.client.request(HttpMethod.GET, path, params={"format": "json"})

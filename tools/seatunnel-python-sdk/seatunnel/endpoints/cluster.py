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


from ..helpers.httpMethod import HttpMethod
from ..helpers.queryParams import OverviewQueryParams
from ..client import Client

class ClusterApi:
    def __init__(self, client: Client):
        self.client = client

    def get_overview(self, params: list[OverviewQueryParams] = []):
        """
        Returns an overview over the Zeta engine cluster.
        """
        return self.client.request(HttpMethod.GET, "/overview")
    
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
    
    def get_logs(self, jobId: int | None = None):
        """
        Get Logs from All Nodes
        """
        return self.client.request(
            HttpMethod.GET, 
            "/logs", 
            params={"jobId": jobId, "format": "json"})
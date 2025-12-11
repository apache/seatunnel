from ..helpers.httpMethod import HttpMethod
from ..helpers.queryParams import OverviewQueryParams

class ClusterApi:
    def __init__(self, client):
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
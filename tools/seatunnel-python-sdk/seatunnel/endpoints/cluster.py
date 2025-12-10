from ..helpers.httpMethod import HttpMethod
from ..helpers.queryParams import OverviewQueryParams

class ClusterApi:
    def __init__(self, client):
        self.client = client

    def getOverview(self, params: list[OverviewQueryParams] = []):
        """
        Returns an overview over the Zeta engine cluster.
        """
        return self.client.request(HttpMethod.GET, "/overview")
    
    def getMetrics(self):
        """
        Get Node Metrics
        """
        return self.client.request(HttpMethod.GET, "/openmetrics")
    
    def getLog(self):
        """
        Get Log Content from a Single Node
        """
        return self.client.request(HttpMethod.GET, "/log")
    
    def getLogs(self, jobId: int | None = None):
        """
        Get Logs from All Nodes
        """
        return self.client.request(
            HttpMethod.GET, 
            "/logs", 
            params={"jobId": jobId, "format": "json"})
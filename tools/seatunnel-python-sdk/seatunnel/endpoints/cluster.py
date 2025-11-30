class ClusterApi:
    def __init__(self, client):
        self.client = client

    def getOverview(self):
        """
        Returns an overview over the Zeta engine cluster.
        """
        return self.client.request("GET", f"/overview")
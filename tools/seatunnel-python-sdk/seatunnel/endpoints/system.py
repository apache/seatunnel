from ..helpers.httpMethod import HttpMethod

class SystemApi:
    def __init__(self, client):
        self.client = client

    def get_system_info(self):
        """
        Returns System Monitoring Information
        """
        return self.client.request(HttpMethod.GET, "/system-monitoring-information")
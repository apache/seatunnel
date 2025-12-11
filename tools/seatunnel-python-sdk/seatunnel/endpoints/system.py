from ..helpers.httpMethod import HttpMethod
from ..client import Client

class SystemApi:
    def __init__(self, client: Client):
        self.client = client

    def get_system_info(self):
        """
        Returns System Monitoring Information
        """
        return self.client.request(HttpMethod.GET, "/system-monitoring-information")
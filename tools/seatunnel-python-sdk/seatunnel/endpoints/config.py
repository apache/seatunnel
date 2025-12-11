from ..helpers.httpMethod import HttpMethod
from ..client import Client

class ConfigApi:
    def __init__(self, client: Client):
        self.client = client

    def encrypt_config(self, conf: str):
        """
        Encrypt Config
        """
        return self.client.request(
            HttpMethod.POST, 
            "/encrypt-config",
            content=conf
        )
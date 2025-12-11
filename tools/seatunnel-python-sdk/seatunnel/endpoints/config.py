from ..helpers.httpMethod import HttpMethod

class ConfigApi:
    def __init__(self, client):
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
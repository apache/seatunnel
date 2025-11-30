class JobsApi:
    def __init__(self, client):
        self.client = client

    def getRunningJobs(self):
        """
        Returns An Overview And State Of All Jobs
        """
        return self.client.request("GET", f"/running-jobs")
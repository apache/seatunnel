from ..helpers.httpMethod import HttpMethod
from ..helpers.submitJobQueryParams import SubmitJobQueryParams

class JobsApi:
    def __init__(self, client):
        self.client = client

    def submitJob(self, conf: str, params: SubmitJobQueryParams):
        """
        Submit A Job
        """
        return self.client.request(HttpMethod.POST, "/submit-job", content=conf, params=params.__dict__)

    def getRunningJobs(self):
        """
        Returns An Overview And State Of All Jobs
        """
        return self.client.request(HttpMethod.GET, "/running-jobs")
    
    def getJobDetails(self, jobId: int):
        """
        Return Details Of A Job
        """
        return self.client.request(HttpMethod.GET, f"/job-info/{jobId}")
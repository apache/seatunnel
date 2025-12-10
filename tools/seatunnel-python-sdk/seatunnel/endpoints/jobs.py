from ..helpers.httpMethod import HttpMethod
from ..helpers.jobStatus import JobStatus
from ..helpers.queryParams import *
import json

class JobsApi:
    def __init__(self, client):
        self.client = client

    def submitJob(self, conf: str, params: SubmitJobQueryParams):
        """
        Submit A Job
        """
        return self.client.request(
            HttpMethod.POST, 
            "/submit-job", 
            content=conf, 
            params=params.__dict__
        )
    
    def submitJobFromFile(self, filePath: str, params: SubmitJobFileQueryParams):
        """
        Submit A Job By Upload Config File
        """
        with open(filePath, "rb") as file:
            files = {"config_file": file}
            return self.client.request(
                HttpMethod.POST,
                "/submit-job/upload",
                files=files,
                params=params.__dict__
            )

    def submitJobs(self, confs: str):
        """
        Batch Submit Jobs
        """
        return self.client.request(
            HttpMethod.POST,
            "/submit-jobs",
            content=confs
        )
    
    def stopJob(self, params: StopJobQueryParams):
        """
        Stop A Job
        """
        jsonBody = json.dumps(params.__dict__)
        return self.client.request(
            HttpMethod.POST,
            "/stop-job",
            content=jsonBody
        )

    def stopJobs(self, params: list[StopJobQueryParams]):
        """
        Batch Stop Jobs
        """
        jsonBody = json.dumps([obj.__dict__ for obj in params])
        return self.client.request(
            HttpMethod.POST,
            "/stop-jobs",
            content=jsonBody
        )

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
    
    def getFinishedJobsInfo(self, state: JobStatus | None = None):
        """
        Return Details Of A Job
        """
        jobStatus = "" if state is None else state.value
        return self.client.request(HttpMethod.GET, f"/finished-jobs/{jobStatus}")
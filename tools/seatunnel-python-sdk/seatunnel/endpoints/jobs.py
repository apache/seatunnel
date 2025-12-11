from ..helpers.httpMethod import HttpMethod
from ..helpers.jobStatus import JobStatus
from ..helpers.queryParams import *
import json

class JobsApi:
    def __init__(self, client):
        self.client = client

    def submit_job(self, conf: str, params: SubmitJobQueryParams):
        """
        Submit A Job
        """
        return self.client.request(
            HttpMethod.POST, 
            "/submit-job", 
            content=conf, 
            params=params.__dict__
        )
    
    def submit_job_from_file(self, filePath: str, params: SubmitJobFileQueryParams):
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

    def submit_jobs(self, confs: str):
        """
        Batch Submit Jobs
        """
        return self.client.request(
            HttpMethod.POST,
            "/submit-jobs",
            content=confs
        )
    
    def stop_job(self, params: StopJobQueryParams):
        """
        Stop A Job
        """
        jsonBody = json.dumps(params.__dict__)
        return self.client.request(
            HttpMethod.POST,
            "/stop-job",
            content=jsonBody
        )

    def stop_jobs(self, params: list[StopJobQueryParams]):
        """
        Batch Stop Jobs
        """
        jsonBody = json.dumps([obj.__dict__ for obj in params])
        return self.client.request(
            HttpMethod.POST,
            "/stop-jobs",
            content=jsonBody
        )

    def get_running_jobs(self):
        """
        Returns An Overview And State Of All Jobs
        """
        return self.client.request(HttpMethod.GET, "/running-jobs")
    
    def get_job_details(self, jobId: int):
        """
        Return Details Of A Job
        """
        return self.client.request(HttpMethod.GET, f"/job-info/{jobId}")
    
    def get_finished_jobs_info(self, state: JobStatus | None = None):
        """
        Return Details Of A Job
        """
        jobStatus = "" if state is None else state.value
        return self.client.request(HttpMethod.GET, f"/finished-jobs/{jobStatus}")
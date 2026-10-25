#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


import json
from typing import Optional

from ..client import Client
from ..helpers.httpMethod import HttpMethod
from ..helpers.jobStatus import JobStatus
from ..helpers.queryParams import (
    StopJobQueryParams,
    SubmitJobFileQueryParams,
    SubmitJobQueryParams,
)


class JobsApi:
    def __init__(self, client: Client):
        self.client = client

    def submit_job(self, conf: str, params: SubmitJobQueryParams):
        """
        Submit A Job
        """
        return self.client.request(
            HttpMethod.POST, "/submit-job", content=conf, params=params.__dict__
        )

    def submit_job_from_file(self, filePath: str, params: SubmitJobFileQueryParams):
        """
        Submit A Job By Upload Config File
        """
        with open(filePath, "rb") as file:
            return self.client.request(
                HttpMethod.POST,
                "/submit-job/upload",
                files={"config_file": file},
                params=params.__dict__,
            )

    def submit_jobs(self, confs: str):
        """
        Batch Submit Jobs
        """
        return self.client.request(HttpMethod.POST, "/submit-jobs", content=confs)

    def stop_job(self, params: StopJobQueryParams):
        """
        Stop A Job
        """
        return self.client.request(HttpMethod.POST, "/stop-job", content=json.dumps(params.__dict__))

    def stop_jobs(self, params: list[StopJobQueryParams]):
        """
        Batch Stop Jobs
        """
        return self.client.request(
            HttpMethod.POST, "/stop-jobs", content=json.dumps([obj.__dict__ for obj in params])
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

    def get_finished_jobs_info(self, state: Optional[JobStatus] = None):
        """
        Return Details Of A Job
        """
        jobStatus = "" if state is None else state.value
        return self.client.request(HttpMethod.GET, f"/finished-jobs/{jobStatus}")

<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->

# SeaTunnel Python SDK

The SeaTunnel Python SDK allows developers to interact with the SeaTunnel Engine using Python. It provides wrappers around the SeaTunnel REST API for job submission, management, and cluster monitoring.

## Installation

To install the SeaTunnel Python SDK, run the following command:

```bash
pip install seatunnel
```

## Usage

### Initialization

First, import the `SeaTunnelClient` and initialize it with the base URL of your SeaTunnel Engine.

```python
from seatunnel import SeaTunnelClient

client = SeaTunnelClient(base_url="http://localhost:8080")
```

### Job Management

You can use the `jobs` property of the client to manage jobs.

#### Submit a Job

```python
from seatunnel import SubmitJobQueryParams

config = """
env {
  job.mode = "batch"
}
source {
  FakeSource {
    plugin_output = "fake"
    row.num = 100
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}
transform {}
sink {
  Console {}
}
"""

query_params = SubmitJobQueryParams()
response = client.jobs.submit_job(conf=config, params=query_params)
print(response)
```

#### Get Running Jobs

```python
running_jobs = client.jobs.get_running_jobs()
print(running_jobs)
```

#### Get Job Details

```python
job_id = 12345  # Replace with actual Job ID
job_details = client.jobs.get_job_details(jobId=job_id)
print(job_details)
```

### Cluster Monitoring

You can use the `cluster` property to get cluster information.

#### Get Cluster Overview

```python
overview = client.cluster.get_overview()
print(overview)
```

## API Reference

### Client

**`SeaTunnelClient(base_url: str)`**

- `base_url`: The address of the SeaTunnel Engine (e.g., `http://127.0.0.1:8080`).

### Jobs (`client.jobs`)

- **`submit_job(conf: str, params: SubmitJobQueryParams)`**: Submit a job with a configuration string.
- **`submit_job_from_file(filePath: str, params: SubmitJobFileQueryParams)`**: Submit a job using a configuration file path.
- **`submit_jobs(confs: str)`**: Submit multiple jobs in batch.
- **`stop_job(params: StopJobQueryParams)`**: Stop a specific job.
- **`stop_jobs(params: list[StopJobQueryParams])`**: Stop multiple jobs.
- **`get_running_jobs()`**: Retrieve a list of currently running jobs.
- **`get_job_details(jobId: int)`**: Retrieve details for a specific job.
- **`get_finished_jobs_info(state: JobStatus | None = None)`**: Retrieve information about finished jobs, optionally filtered by status.

### Cluster (`client.cluster`)

- **`get_overview(params: list[OverviewQueryParams] = [])`**: Get the cluster overview.
- **`get_metrics()`**: Get cluster metrics.
- **`get_log()`**: Get logs from a single node.
- **`get_logs(jobId: int | None = None)`**: Get logs from all nodes, optionally filtered by Job ID.

### Helper Classes

- **`SubmitJobQueryParams`**: Parameters for submitting a job.
- **`SubmitJobFileQueryParams`**: Parameters for submitting a job from a file.
- **`StopJobQueryParams`**: Parameters for stopping a job.
- **`JobStatus`**: Enum for job status.
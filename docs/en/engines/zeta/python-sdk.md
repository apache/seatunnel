# SeaTunnel Python SDK

The SeaTunnel Python SDK allows developers to interact with the SeaTunnel Engine using Python. It provides wrappers around the SeaTunnel REST API for job submission, management, and cluster monitoring.

## Installation

The SeaTunnel Python SDK is not published to PyPI yet. Install it from the SeaTunnel source tree:

```bash
git clone https://github.com/apache/seatunnel.git
cd seatunnel
python -m pip install ./tools/seatunnel-python-sdk
```

Python 3.9 or newer is required.

## Usage

### Job Management

You can use the `jobs` property of the client to manage jobs.

#### Submit a Job

```python
from seatunnel import SeaTunnelClient, SubmitJobQueryParams

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
with SeaTunnelClient(base_url="http://localhost:8080") as client:
  query_params = SubmitJobQueryParams()
  response = client.jobs.submit_job(conf=config, params=query_params)
  print(response)
```

#### Get Running Jobs

```python
with SeaTunnelClient(base_url="http://localhost:8080") as client:
  running_jobs = client.jobs.get_running_jobs()
  print(running_jobs)
```

#### Get Job Details

```python
with SeaTunnelClient(base_url="http://localhost:8080") as client:
  job_id = 12345  # Replace with actual Job ID
  job_details = client.jobs.get_job_details(jobId=job_id)
  print(job_details)
```

### Cluster Monitoring

You can use the `cluster` property to get cluster information.

#### Get Cluster Overview

```python
with SeaTunnelClient(base_url="http://localhost:8080") as client:
  overview = client.cluster.get_overview({"jobId": "12345"})
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
- **`get_finished_jobs_info(state: Optional[JobStatus] = None)`**: Retrieve information about finished jobs, optionally filtered by status.

### Cluster (`client.cluster`)

- **`get_overview(params: Optional[dict[str, str]] = None)`**: Get the cluster overview with optional tag filters forwarded as query parameters.
- **`get_metrics()`**: Get cluster metrics.
- **`get_log()`**: Get logs from a single node.
- **`get_logs(jobId: Optional[int] = None)`**: Get logs from all nodes, optionally filtered by Job ID.

### Helper Classes

- **`SubmitJobQueryParams`**: Parameters for submitting a job.
- **`SubmitJobFileQueryParams`**: Parameters for submitting a job from a file.
- **`StopJobQueryParams`**: Parameters for stopping a job.
- **`JobStatus`**: Enum for job status.

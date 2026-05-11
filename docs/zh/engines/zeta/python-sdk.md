# SeaTunnel Python SDK

SeaTunnel Python SDK 允许开发者使用 Python 与 SeaTunnel Engine 进行交互。它提供了围绕 SeaTunnel REST API 的封装，用于作业提交、管理和集群监控。

## 安装

SeaTunnel Python SDK 目前还没有发布到 PyPI，需要直接从 SeaTunnel 源码目录安装：

```bash
git clone https://github.com/apache/seatunnel.git
cd seatunnel
python -m pip install ./tools/seatunnel-python-sdk
```

需要 Python 3.9 或更高版本。

## 使用方法

### 作业管理

您可以使用客户端的 `jobs` 属性来管理作业。

#### 提交作业

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

#### 获取运行中的作业

```python
with SeaTunnelClient(base_url="http://localhost:8080") as client:
  running_jobs = client.jobs.get_running_jobs()
  print(running_jobs)
```

#### 获取作业详情

```python
with SeaTunnelClient(base_url="http://localhost:8080") as client:
  job_id = 12345  # 替换为实际的 Job ID
  job_details = client.jobs.get_job_details(jobId=job_id)
  print(job_details)
```

### 集群监控

您可以使用 `cluster` 属性来获取集群信息。

#### 获取集群概览

```python
with SeaTunnelClient(base_url="http://localhost:8080") as client:
  overview = client.cluster.get_overview({"jobId": "12345"})
  print(overview)
```

## API 参考

### Client (客户端)

**`SeaTunnelClient(base_url: str)`**

- `base_url`: SeaTunnel Engine 的地址 (例如：`http://127.0.0.1:8080`)。

### Jobs (`client.jobs`)

- **`submit_job(conf: str, params: SubmitJobQueryParams)`**: 使用配置字符串提交作业。
- **`submit_job_from_file(filePath: str, params: SubmitJobFileQueryParams)`**: 使用配置文件路径提交作业。
- **`submit_jobs(confs: str)`**: 批量提交多个作业。
- **`stop_job(params: StopJobQueryParams)`**: 停止特定作业。
- **`stop_jobs(params: list[StopJobQueryParams])`**: 停止多个作业。
- **`get_running_jobs()`**: 获取当前正在运行的作业列表。
- **`get_job_details(jobId: int)`**: 获取特定作业的详细信息。
- **`get_finished_jobs_info(state: Optional[JobStatus] = None)`**: 获取已完成作业的信息，可按状态过滤。

### Cluster (`client.cluster`)

- **`get_overview(params: Optional[dict[str, str]] = None)`**: 获取集群概览，并将过滤标签作为查询参数透传给 REST API。
- **`get_metrics()`**: 获取集群指标。
- **`get_log()`**: 获取单个节点的日志。
- **`get_logs(jobId: Optional[int] = None)`**: 获取所有节点的日志，可按 Job ID 过滤。

### Helper Classes (辅助类)

- **`SubmitJobQueryParams`**: 提交作业的参数。
- **`SubmitJobFileQueryParams`**: 从文件提交作业的参数。
- **`StopJobQueryParams`**: 停止作业的参数。
- **`JobStatus`**: 作业状态枚举。

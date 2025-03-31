"""SeaTunnel API 客户端集成测试。

注意：这些测试需要一个运行中的 SeaTunnel 实例。
您可以使用 Docker 启动一个测试实例：

```bash
docker run -d --name seatunnel -p 8090:8090 apache/seatunnel:latest
```

或者，您可以在测试中使用模拟服务器。
"""

import os
import pytest
import httpx
from unittest.mock import patch

from src.seatunnel_mcp.client import SeaTunnelClient


# 跳过集成测试，除非明确启用
pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_INTEGRATION_TESTS") != "1",
    reason="需要设置 ENABLE_INTEGRATION_TESTS=1 环境变量来运行集成测试"
)

# SeaTunnel API URL，可以通过环境变量覆盖
API_URL = os.environ.get("SEATUNNEL_API_URL", "http://localhost:8090")
API_KEY = os.environ.get("SEATUNNEL_API_KEY", None)


@pytest.fixture
def client():
    """创建一个 SeaTunnel 客户端实例用于测试。"""
    return SeaTunnelClient(base_url=API_URL, api_key=API_KEY)


def test_connection(client):
    """测试与 SeaTunnel API 的连接。"""
    try:
        # 获取概览信息（这是一个简单的端点，通常可用）
        response = client.get_overview()
        assert isinstance(response, dict)
    except httpx.RequestError as e:
        pytest.skip(f"无法连接到 SeaTunnel API: {e}")
    except httpx.HTTPStatusError as e:
        pytest.skip(f"SeaTunnel API 响应错误: {e}")


def test_get_running_jobs(client):
    """测试获取运行中的作业。"""
    try:
        response = client.get_running_jobs()
        assert isinstance(response, dict)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            pytest.skip("get_running_jobs 端点不可用")
        raise


def test_get_finished_jobs(client):
    """测试获取已完成的作业。"""
    try:
        response = client.get_finished_jobs(state="FINISHED")
        assert isinstance(response, dict)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            pytest.skip("get_finished_jobs 端点不可用")
        raise


def test_get_system_monitoring_information(client):
    """测试获取系统监控信息。"""
    try:
        response = client.get_system_monitoring_information()
        assert isinstance(response, dict)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            pytest.skip("get_system_monitoring_information 端点不可用")
        raise


def test_submit_and_stop_job(client):
    """测试提交和停止作业。
    
    注意：这个测试会提交一个真实的作业，可能会消耗资源。
    """
    # 定义一个简单的测试作业配置
    job_config = """
    env {
      job.mode = "batch"
    }
    
    source {
      FakeSource {
        row.num = 10
        schema = {
          fields {
            id = int
            name = string
          }
        }
      }
    }
    
    sink {
      Console {}
    }
    """
    
    try:
        # 提交作业
        submit_response = client.submit_job(
            job_content=job_config,
            job_name="integration_test_job",
            format="hocon"
        )
        assert isinstance(submit_response, dict)
        
        # 检查是否获取到作业 ID
        assert "jobId" in submit_response or "jobId" in submit_response
        
        # 获取作业 ID
        jobId = submit_response.get("jobId", submit_response.get("jobId"))
        
        # 尝试停止作业（可能已经完成）
        try:
            stop_response = client.stop_job(jobId=jobId)
            assert isinstance(stop_response, dict)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                # 作业可能已经完成，这是正常的
                pass
            elif e.response.status_code == 400:
                # 作业可能已经停止，这也是正常的
                pass
            else:
                raise
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            pytest.skip("submit_job 或 stop_job 端点不可用")
        raise 
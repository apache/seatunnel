# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Tests for the SeaTunnel MCP tools."""

import pytest
from unittest.mock import MagicMock

from src.seatunnel_mcp.client import SeaTunnelClient
from src.seatunnel_mcp.tools import (
    get_connection_settings_tool,
    update_connection_settings_tool,
    submit_job_tool,
    stop_job_tool,
    get_job_info_tool,
    get_running_job_tool,
    get_running_jobs_tool,
    get_finished_jobs_tool,
    get_overview_tool,
    get_system_monitoring_information_tool,
    get_all_tools,
)


@pytest.fixture
def mock_client():
    """Create a mock client for testing."""
    client = MagicMock(spec=SeaTunnelClient)
    client.get_connection_settings.return_value = {
        "url": "http://localhost:8090",
        "has_api_key": True,
    }
    client.update_connection_settings.return_value = {
        "url": "http://new-host:8090",
        "has_api_key": True,
    }
    client.submit_job.return_value = {"jobId": "123"}
    client.stop_job.return_value = {"status": "success"}
    client.get_job_info.return_value = {"jobId": "123", "status": "RUNNING"}
    client.get_running_job.return_value = {"jobId": "123", "status": "RUNNING"}
    client.get_running_jobs.return_value = {"jobs": [{"jobId": "123", "status": "RUNNING"}]}
    client.get_finished_jobs.return_value = {"jobs": [{"jobId": "456", "status": "FINISHED"}]}
    client.get_overview.return_value = {"cluster": "info"}
    client.get_system_monitoring_information.return_value = {"system": "info"}
    return client


@pytest.mark.asyncio
async def test_get_connection_settings_tool(mock_client):
    """Test get_connection_settings_tool."""
    tool = get_connection_settings_tool(mock_client)
    assert tool.name == "get-connection-settings"
    result = await tool.fn()
    mock_client.get_connection_settings.assert_called_once()
    assert result == {
        "url": "http://localhost:8090",
        "has_api_key": True,
    }


@pytest.mark.asyncio
async def test_update_connection_settings_tool(mock_client):
    """Test update_connection_settings_tool."""
    tool = update_connection_settings_tool(mock_client)
    assert tool.name == "update-connection-settings"
    result = await tool.fn(url="http://new-host:8090", api_key="new_key")
    mock_client.update_connection_settings.assert_called_once_with(
        url="http://new-host:8090", api_key="new_key"
    )
    assert result == {
        "url": "http://new-host:8090",
        "has_api_key": True,
    }


@pytest.mark.asyncio
async def test_submit_job_tool(mock_client):
    """Test submit_job_tool."""
    tool = submit_job_tool(mock_client)
    assert tool.name == "submit-job"
    job_content = "env { job.mode = \"batch\" }"
    result = await tool.fn(
        job_content=job_content,
        jobName="test_job",
        format="hocon",
    )
    mock_client.submit_job.assert_called_once_with(
        job_content=job_content,
        jobName="test_job",
        jobId=None,
        is_start_with_save_point=None,
        format="hocon",
    )
    assert result == {"jobId": "123"}


def test_get_all_tools(mock_client):
    """Test get_all_tools."""
    tools = get_all_tools(mock_client)
    assert len(tools) == 10
    tool_names = [tool.__name__ for tool in tools]
    assert "get-connection-settings" in tool_names
    assert "update-connection-settings" in tool_names
    assert "submit-job" in tool_names
    assert "stop-job" in tool_names
    assert "get-job-info" in tool_names
    assert "get-running-job" in tool_names
    assert "get-running-jobs" in tool_names
    assert "get-finished-jobs" in tool_names
    assert "get-overview" in tool_names
    assert "get-system-monitoring-information" in tool_names 
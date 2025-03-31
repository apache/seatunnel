"""Tests for the SeaTunnel client."""

import pytest
import httpx
from unittest.mock import patch, MagicMock

from src.seatunnel_mcp.client import SeaTunnelClient


@pytest.fixture
def client():
    """Create a client for testing."""
    return SeaTunnelClient(base_url="http://localhost:8090", api_key="test_key")


def test_init(client):
    """Test client initialization."""
    assert client.base_url == "http://localhost:8090"
    assert client.api_key == "test_key"
    assert client.headers == {
        "Content-Type": "application/json",
        "Authorization": "Bearer test_key",
    }


def test_get_connection_settings(client):
    """Test get_connection_settings."""
    settings = client.get_connection_settings()
    assert settings == {
        "url": "http://localhost:8090",
        "has_api_key": True,
    }


def test_update_connection_settings(client):
    """Test update_connection_settings."""
    settings = client.update_connection_settings(
        url="http://new-host:8090",
        api_key="new_key",
    )
    assert client.base_url == "http://new-host:8090"
    assert client.api_key == "new_key"
    assert client.headers["Authorization"] == "Bearer new_key"
    assert settings == {
        "url": "http://new-host:8090",
        "has_api_key": True,
    }


@patch("httpx.Client")
def test_submit_job(mock_client, client):
    """Test submit_job."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"jobId": "123"}
    mock_response.raise_for_status.return_value = None
    
    mock_client_instance = MagicMock()
    mock_client_instance.request.return_value = mock_response
    mock_client.return_value.__enter__.return_value = mock_client_instance

    job_content = "env { job.mode = \"batch\" }"
    result = client.submit_job(
        job_content=job_content,
        job_name="test_job",
        format="hocon",
    )

    mock_client_instance.request.assert_called_once_with(
        "POST",
        "http://localhost:8090/submit-job",
        headers={
            "Content-Type": "text/plain",
            "Authorization": "Bearer test_key",
        },
        params={"jobName": "test_job", "format": "hocon"},
        content=job_content,
    )
    
    assert result == {"jobId": "123"} 
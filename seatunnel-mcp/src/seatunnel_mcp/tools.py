"""MCP tools for interacting with the SeaTunnel REST API."""

import json
import logging
from typing import Dict, List, Any, Optional, Union, Callable, Iterable
from functools import wraps

from mcp.server.fastmcp.tools import Tool
from mcp.types import TextContent, ImageContent, EmbeddedResource

from .client import SeaTunnelClient

logger = logging.getLogger(__name__)


def get_connection_settings_tool(client: SeaTunnelClient) -> Callable:
    """Get a tool for retrieving connection settings.

    Args:
        client: SeaTunnel client instance.

    Returns:
        Function that can be registered as a tool.
    """
    async def get_connection_settings() -> Dict[str, Any]:
        """Get current connection settings."""
        result = client.get_connection_settings()
        return result

    get_connection_settings.__name__ = "get-connection-settings"
    get_connection_settings.__doc__ = "Get current SeaTunnel connection URL and API key status"
    
    return get_connection_settings


def update_connection_settings_tool(client: SeaTunnelClient) -> Callable:
    """Get a tool for updating connection settings.

    Args:
        client: SeaTunnel client instance.

    Returns:
        Function that can be registered as a tool.
    """
    async def update_connection_settings(url: Optional[str] = None, api_key: Optional[str] = None) -> Dict[str, Any]:
        """Update connection settings.

        Args:
            url: New base URL for the SeaTunnel REST API.
            api_key: New API key for authentication.

        Returns:
            Updated connection settings.
        """
        result = client.update_connection_settings(url=url, api_key=api_key)
        return result
    
    update_connection_settings.__name__ = "update-connection-settings"
    update_connection_settings.__doc__ = "Update URL and/or API key to connect to a different SeaTunnel instance"
    
    return update_connection_settings


def submit_job_tool(client: SeaTunnelClient) -> Callable:
    """Get a tool for submitting a job.

    Args:
        client: SeaTunnel client instance.

    Returns:
        Function that can be registered as a tool.
    """
    async def submit_job(
        job_content: str,
        job_name: Optional[str] = None,
        jobId: Optional[str] = None,
        is_start_with_save_point: Optional[bool] = None,
        format: str = "hocon",
    ) -> Dict[str, Any]:
        """Submit a new job.

        Args:
            job_content: Job configuration content.
            job_name: Optional job name. In the API, this parameter is named 'jobName'.
            jobId: Optional job ID.
            is_start_with_save_point: Whether to start with savepoint. In the API, this parameter is named 'isStartWithSavePoint'.
            format: Job configuration format (hocon, json, yaml).

        Returns:
            Response from the API.
        """
        result = client.submit_job(
            job_content=job_content,
            job_name=job_name,
            jobId=jobId,
            is_start_with_save_point=is_start_with_save_point,
            format=format,
        )
        return result
    
    submit_job.__name__ = "submit-job"
    submit_job.__doc__ = "Submit a new job to the SeaTunnel cluster with configuration content"
    
    return submit_job


def stop_job_tool(client: SeaTunnelClient) -> Callable:
    """Get a tool for stopping a running job.

    Args:
        client: SeaTunnel client instance.

    Returns:
        Function that can be registered as a tool.
    """
    async def stop_job(jobId: Union[str, int], is_stop_with_save_point: bool = False) -> Dict[str, Any]:
        """Stop a running job.

        Args:
            jobId: Job ID. Can be a string or integer.
            is_stop_with_save_point: Whether to stop with savepoint.

        Returns:
            Response from the API.
        """
        result = client.stop_job(jobId=jobId, is_stop_with_save_point=is_stop_with_save_point)
        return result
    
    stop_job.__name__ = "stop-job"
    stop_job.__doc__ = "Stop a running job by providing the jobId and optional is_stop_with_save_point flag"
    
    return stop_job


def get_job_info_tool(client: SeaTunnelClient) -> Callable:
    """Get a tool for retrieving job information.

    Args:
        client: SeaTunnel client instance.

    Returns:
        Function that can be registered as a tool.
    """
    async def get_job_info(jobId: Union[str, int]) -> Dict[str, Any]:
        """Get information about a job.

        Args:
            jobId: Job ID (used as path parameter in /job-info/{jobId}). Can be a string or integer.

        Returns:
            Response from the API.
        """
        result = client.get_job_info(jobId=jobId)
        return result
    
    get_job_info.__name__ = "get-job-info"
    get_job_info.__doc__ = "Get detailed information about a specific job by providing the jobId as a path parameter"
    
    return get_job_info


def get_running_job_tool(client: SeaTunnelClient) -> Callable:
    """Get a tool for retrieving information about a running job.

    Args:
        client: SeaTunnel client instance.

    Returns:
        Function that can be registered as a tool.
    """
    async def get_running_job(jobId: Union[str, int]) -> Dict[str, Any]:
        """Get information about a running job.

        Args:
            jobId: Job ID (used as path parameter in /running-job/{jobId}). Can be a string or integer.

        Returns:
            Response from the API.
        """
        result = client.get_running_job(jobId=jobId)
        return result
    
    get_running_job.__name__ = "get-running-job"
    get_running_job.__doc__ = "Get details about a specific running job by providing the jobId as a path parameter"
    
    return get_running_job


def get_running_jobs_tool(client: SeaTunnelClient) -> Callable:
    """Get a tool for retrieving all running jobs.

    Args:
        client: SeaTunnel client instance.

    Returns:
        Function that can be registered as a tool.
    """
    async def get_running_jobs() -> Dict[str, Any]:
        """Get all running jobs.

        Returns:
            Response from the API.
        """
        result = client.get_running_jobs()
        return result
    
    get_running_jobs.__name__ = "get-running-jobs"
    get_running_jobs.__doc__ = "List all currently running jobs"
    
    return get_running_jobs


def get_finished_jobs_tool(client: SeaTunnelClient) -> Callable:
    """Get a tool for retrieving all finished jobs by state.

    Args:
        client: SeaTunnel client instance.

    Returns:
        Function that can be registered as a tool.
    """
    async def get_finished_jobs(state: str) -> Dict[str, Any]:
        """Get all finished jobs by state.

        Args:
            state: Job state (FINISHED, CANCELED, FAILED, UNKNOWABLE) (used as path parameter in /finished-jobs/{state}).

        Returns:
            Response from the API.
        """
        result = client.get_finished_jobs(state=state)
        return result
    
    get_finished_jobs.__name__ = "get-finished-jobs"
    get_finished_jobs.__doc__ = "List all finished jobs by state (FINISHED, CANCELED, FAILED, UNKNOWABLE) using the state as a path parameter"
    
    return get_finished_jobs


def get_overview_tool(client: SeaTunnelClient) -> Callable:
    """Get a tool for retrieving cluster overview.

    Args:
        client: SeaTunnel client instance.

    Returns:
        Function that can be registered as a tool.
    """
    async def get_overview(tags: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Get cluster overview.

        Args:
            tags: Optional tags for filtering.

        Returns:
            Response from the API.
        """
        result = client.get_overview(tags=tags)
        return result
    
    get_overview.__name__ = "get-overview"
    get_overview.__doc__ = "Get an overview of the SeaTunnel cluster"
    
    return get_overview


def get_system_monitoring_information_tool(client: SeaTunnelClient) -> Callable:
    """Get a tool for retrieving system monitoring information.

    Args:
        client: SeaTunnel client instance.

    Returns:
        Function that can be registered as a tool.
    """
    async def get_system_monitoring_information() -> Dict[str, Any]:
        """Get system monitoring information.

        Returns:
            Response from the API.
        """
        result = client.get_system_monitoring_information()
        return result
    
    get_system_monitoring_information.__name__ = "get-system-monitoring-information"
    get_system_monitoring_information.__doc__ = "Get detailed system monitoring information"
    
    return get_system_monitoring_information


def get_all_tools(client: SeaTunnelClient) -> List[Callable]:
    """Get all MCP tools.

    Args:
        client: SeaTunnelClient instance.

    Returns:
        List of all tool functions.
    """
    return [
        get_connection_settings_tool(client),
        update_connection_settings_tool(client),
        submit_job_tool(client),
        stop_job_tool(client),
        get_job_info_tool(client),
        get_running_job_tool(client),
        get_running_jobs_tool(client),
        get_finished_jobs_tool(client),
        get_overview_tool(client),
        get_system_monitoring_information_tool(client),
    ] 
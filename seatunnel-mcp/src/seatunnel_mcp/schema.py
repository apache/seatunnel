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

"""MCP schemas for the SeaTunnel MCP tools."""

from typing import Dict, List, Any, Optional, Union
from pydantic import BaseModel, Field


class ConnectionSettings(BaseModel):
    """Connection settings for the SeaTunnel API."""

    url: str = Field(..., description="Base URL of the SeaTunnel REST API")
    has_api_key: bool = Field(..., description="Whether an API key is set")


class UpdateConnectionSettings(BaseModel):
    """Update connection settings for the SeaTunnel API."""

    url: Optional[str] = Field(None, description="New base URL for the SeaTunnel REST API")
    api_key: Optional[str] = Field(None, description="New API key for authentication")


class SubmitJobRequest(BaseModel):
    """Request for submitting a job."""

    job_content: str = Field(..., description="Job configuration content in specified format")
    jobName: Optional[str] = Field(None, description="Optional job name")
    jobId: Optional[str] = Field(None, description="Optional job ID")
    is_start_with_save_point: Optional[bool] = Field(None, description="Whether to start with savepoint")
    format: str = Field("hocon", description="Job configuration format (hocon, json, yaml)")


class StopJobRequest(BaseModel):
    """Request for stopping a job."""

    jobId: Union[str, int] = Field(..., description="Job ID")
    isStartWithSavePoint: bool = Field(False, description="Whether to stop with savepoint")


class JobInfoRequest(BaseModel):
    """Request for getting job information."""

    jobId: Union[str, int] = Field(..., description="Job ID")


class FinishedJobsRequest(BaseModel):
    """Request for getting finished jobs."""

    state: str = Field(..., description="Job state (FINISHED, CANCELED, FAILED, UNKNOWABLE)")


class OverviewRequest(BaseModel):
    """Request for getting cluster overview."""

    tags: Optional[Dict[str, str]] = Field(None, description="Optional tags for filtering") 
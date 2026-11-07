#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Tests for pending clarification state across session changes."""

from unittest.mock import Mock

import pytest

from seatunnel_cli.cli import SeaTunnelCLI


@pytest.fixture
def cli():
    instance = SeaTunnelCLI.__new__(SeaTunnelCLI)
    instance.console = Mock()
    instance.session_manager = Mock()
    instance.orchestrator = Mock()
    instance.orchestrator.conversation_history = []
    instance.last_config = "old config"
    instance._pending_request = "Sync old source to old sink"
    instance.client = Mock()
    return instance


@pytest.mark.parametrize("command", ["/new", "/clear", "/resume other"])
def test_successful_session_change_discards_pending_clarification(cli, command):
    cli.session_manager.load_session.return_value = ([], "resumed config")
    cli._handle_command(command)
    assert cli._pending_request is None


def test_failed_resume_preserves_pending_clarification(cli):
    cli.session_manager.load_session.side_effect = FileNotFoundError("not found")
    cli._cmd_resume("missing")
    assert cli._pending_request == "Sync old source to old sink"
    assert cli.last_config == "old config"


def test_non_session_command_preserves_pending_clarification(cli):
    cli._handle_command("/help")
    assert cli._pending_request == "Sync old source to old sink"

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

"""Tests for credential redaction in conversation summaries."""

import json
from unittest.mock import Mock

from seatunnel_cli.memory import SessionManager


def test_summary_redacts_conversation_before_model_and_persisted_response(tmp_path):
    manager = SessionManager(tmp_path)
    history = [
        {
            "role": "user",
            "content": [{"text": "Connect with password=exampleSecret123"}],
        },
        {
            "role": "assistant",
            "content": [{"text": "Use JDBC with password=exampleSecret123"}],
        },
    ]
    client = Mock()
    client.quick_chat.return_value = "JDBC password is responseSecret456"
    manager.save_session(history)
    summary = manager.generate_summary(history, client)
    manager.update_summary(summary)

    assert "exampleSecret123" not in client.quick_chat.call_args.args[0]
    assert "responseSecret456" not in summary
    saved = (manager.sessions_dir / f"{manager.current_session_id}.json").read_text()
    assert "exampleSecret123" not in saved
    assert "responseSecret456" not in saved
    assert "REDACTED" in json.loads(saved)["summary"]
    assert history[0]["content"][0]["text"].endswith("exampleSecret123")


def test_direct_summary_update_redacts_credentials_and_preserves_other_fields(tmp_path):
    manager = SessionManager(tmp_path)
    manager.save_session([], "source { Jdbc { url=localhost } }")
    manager.update_summary("JDBC password=directSecret789 at localhost")
    data = json.loads(
        (manager.sessions_dir / f"{manager.current_session_id}.json").read_text()
    )
    assert "directSecret789" not in data["summary"]
    assert "localhost" in data["summary"]
    assert data["last_config"] == "source { Jdbc { url=localhost } }"


def test_summary_redacts_before_truncating_long_credential(tmp_path):
    manager = SessionManager(tmp_path)
    secret = "x" * 230
    history = [
        {"role": "user", "content": [{"text": "password=" + secret}]},
        {"role": "assistant", "content": [{"text": "JDBC"}]},
    ]
    client = Mock()
    client.quick_chat.return_value = "JDBC conversation"
    manager.generate_summary(history, client)
    assert "x" * 20 not in client.quick_chat.call_args.args[0]
    assert "REDACTED" in client.quick_chat.call_args.args[0]

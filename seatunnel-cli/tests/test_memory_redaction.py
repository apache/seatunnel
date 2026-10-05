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

"""Tests for complete credential assignment redaction."""

import json

import pytest

from seatunnel_cli.memory import SessionManager, contains_credential, redact_credentials


@pytest.mark.parametrize(
    "text",
    [
        '"password": "example secret:123"',
        "'api_key' = 'example secret:123'",
        'password = "example secret:123"',
        'password is "example secret:123"',
        'password = "example \\"quote\\" secret"',
        "password = ab",
    ],
)
def test_redacts_entire_quoted_credential_value(text):
    redacted = redact_credentials(text)
    assert contains_credential(text)
    assert "example" not in redacted
    assert "secret" not in redacted
    assert "123" not in redacted
    assert "quote" not in redacted
    assert not redacted.endswith("ab")
    assert "REDACTED" in redacted


def test_session_json_redacts_quoted_keys_and_keeps_structure(tmp_path):
    manager = SessionManager(tmp_path)
    config = '{"password":"two words:123","host":"localhost"}'
    manager.save_session([{"role": "user", "content": [{"text": config}]}], config)
    raw = (manager.sessions_dir / f"{manager.current_session_id}.json").read_text()
    assert "two words:123" not in raw
    data = json.loads(raw)
    assert json.loads(data["last_config"]) == {
        "password": "***REDACTED***",
        "host": "localhost",
    }


def test_redaction_does_not_change_key_when_secret_equals_key():
    assert redact_credentials("password=password") == "password=***REDACTED***"


@pytest.mark.parametrize(
    "key",
    [
        "jdbc_password",
        "client_secret",
        "access_secret",
        "refresh_token",
        "access_key_secret",
        "secret_access_key",
        "MYSQL_PASSWORD",
    ],
)
@pytest.mark.parametrize("quoted", [False, True])
def test_preserves_redaction_for_prefixed_connector_credential_keys(key, quoted):
    text = f'"{key}": "prefixed value:123"' if quoted else f"{key}=prefixedSecret123"
    assert contains_credential(text)
    redacted = redact_credentials(text)
    assert "prefixed" not in redacted
    assert key in redacted

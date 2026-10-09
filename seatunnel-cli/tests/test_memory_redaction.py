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
import time

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


@pytest.mark.parametrize(
    "text, key, secret",
    [
        ('password="correct-horse-battery', "password", "correct-horse-battery"),
        ("password: 'abcdef123456", "password", "abcdef123456"),
        ('client_secret="abcdef123456', "client_secret", "abcdef123456"),
        ("MYSQL_PASSWORD='two words:123", "MYSQL_PASSWORD", "two words:123"),
        ('password is "abc123secret', "password", "abc123secret"),
        ("api_key was 'abcd 1234:efgh", "api_key", "abcd 1234:efgh"),
    ],
)
def test_redacts_unterminated_quoted_credential_value(text, key, secret):
    # Truncated text still holds a real secret, so an opening quote without a
    # closing partner must not stop the value from being redacted. The key
    # itself stays readable, which is what makes a failure report diagnosable.
    redacted = redact_credentials(text)
    assert secret not in redacted
    assert key in redacted
    assert "REDACTED" in redacted


@pytest.mark.parametrize(
    "text, config",
    [
        (
            'password="\nsource { Jdbc { url="jdbc:mysql://h:3306/db" user="u" } }\n',
            'source { Jdbc { url="jdbc:mysql://h:3306/db" user="u" } }',
        ),
        (
            'password="\n  host="db.internal"\n  port: 3306',
            '  host="db.internal"',
        ),
        (
            "token: '\n  host: 'db.internal'\n  port: 3306",
            "  host: 'db.internal'",
        ),
    ],
)
def test_dangling_quote_does_not_swallow_next_line_config(text, config):
    # A quoted value lives on one line: a dangling opening quote must not pair
    # with an unrelated quote further down and delete the configuration in
    # between, which silently loses stored history and summaries.
    assert config in redact_credentials(text)


def test_unterminated_value_with_spaces_stops_at_end_of_line():
    text = 'password="s3cr3t value, with punctuation!\nhost=localhost\nport=3306'
    redacted = redact_credentials(text)
    assert "s3cr3t" not in redacted
    assert "punctuation" not in redacted
    assert "host=localhost" in redacted
    assert "port=3306" in redacted


def test_session_json_redacts_unterminated_quoted_value_in_last_config(tmp_path):
    manager = SessionManager(tmp_path)
    config = 'password="two words:123\nhost=localhost'
    manager.save_session([{"role": "user", "content": [{"text": config}]}], config)
    raw = (manager.sessions_dir / f"{manager.current_session_id}.json").read_text()
    assert "two words:123" not in raw
    assert "host=localhost" in json.loads(raw)["last_config"]


def test_long_credential_free_word_run_is_not_rescanned_per_offset():
    # Every key pattern starts with a `[\w.-]*` run. Without a left boundary the
    # engine restarts that run at every offset of this line, which made a single
    # 30000-character line take about a minute to redact.
    text = "a" * 30000
    start = time.perf_counter()
    assert redact_credentials(text) == text
    elapsed = time.perf_counter() - start
    assert not contains_credential(text)
    # Keep a wide margin for slow CI machines: the fixed scan is milliseconds,
    # while the regression took about a minute in the local reproduction.
    assert elapsed < 5.0, f"redacting 30000 word characters took {elapsed:.2f}s"


def test_long_prefixed_key_is_still_redacted_within_the_run():
    # The boundary must not stop a genuine prefixed key from being redacted when
    # it sits at the end of a long `[\w.-]` run instead of after a space.
    secret = "prefixedSecret123"
    text = "a" * 30000 + f"jdbc_password={secret}"
    start = time.perf_counter()
    redacted = redact_credentials(text)
    elapsed = time.perf_counter() - start
    assert secret not in redacted
    assert "jdbc_password" in redacted
    assert "REDACTED" in redacted
    assert elapsed < 5.0, f"redacting a long prefixed key took {elapsed:.2f}s"

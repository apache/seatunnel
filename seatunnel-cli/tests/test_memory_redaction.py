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


@pytest.mark.parametrize(
    "text",
    [
        "password = ${DB_PASSWORD}",
        "password=${DB_PASSWORD}",
        "password = ${?MYSQL_PASSWORD}",
        "password is ${DB_PASSWORD}",
        "password was ${?DB_PASSWORD}",
        '"password": ${DB_PASSWORD}',
    ],
)
def test_keeps_bare_hocon_substitution(text):
    # Regression: the bare value alternative accepted the single `$` that opens a
    # substitution, so `password = ${DB_PASSWORD}` was stored as
    # `password = ***REDACTED***{DB_PASSWORD}`. The stored config is reloaded
    # after /resume and written out again by /save and /export, so that
    # corruption reached the engine as invalid HOCON.
    assert redact_credentials(text) == text


@pytest.mark.parametrize(
    "text",
    [
        'password = "${DB_PASSWORD}"',
        "password = '${DB_PASSWORD}'",
        'password = "${?MYSQL_PASSWORD}"',
        '"password": "${DB_PASSWORD}"',
        "'api_key' = '${?API_KEY}'",
        'password = " ${DB_PASSWORD} "',
    ],
)
def test_keeps_quoted_hocon_substitution(text):
    # A quoted value that is nothing but a reference keeps the secret out of the
    # config file, so it is a reference and not a secret.
    assert redact_credentials(text) == text


@pytest.mark.parametrize(
    "text",
    [
        "password = ${DB_PASSWORD}${?OTHER}",
        "password=${A}${B}",
        "password = ${?DB_PASSWORD}${?MYSQL_PASSWORD}",
        "password = ${A}${B}${C}",
        "password is ${A}${B}",
        'password = "${DB_PASSWORD}${?OTHER}"',
        "password = '${DB_PASSWORD}${OTHER}'",
        '"password": ${DB_PASSWORD}${OTHER},',
        "password = ${A}${B} # comment",
    ],
)
def test_keeps_concatenated_hocon_substitutions(text):
    # HOCON concatenates whitespace-free values, so a chain of references is
    # still nothing but references. Redacting it would replace the whole chain
    # with a marker and break the config that is reloaded after /resume.
    assert redact_credentials(text) == text


@pytest.mark.parametrize(
    "text",
    [
        'password = ""',
        "password = ''",
        '"password": ""',
        "token = ''",
        'password = "" # comment',
        '"password": "",',
        "password = '' }",
        "token = ''   ",
        'password = ""\nhost=localhost',
    ],
)
def test_keeps_empty_quoted_value(text):
    # An empty value carries no secret, so it stays empty instead of being
    # rewritten into a non-empty marker inside the stored config. It is the
    # whole value on its line, which is what makes it an empty credential.
    assert redact_credentials(text) == text


@pytest.mark.parametrize(
    "text, expected",
    [
        (
            'private_key = """-----BEGIN RSA PRIVATE KEY-----',
            'private_key = "***REDACTED***""-----BEGIN RSA PRIVATE KEY-----',
        ),
        ('password = ""s3cret', 'password = "***REDACTED***"s3cret'),
        ("password = ''s3cret", "password = '***REDACTED***'s3cret"),
        ('password = "" ""', 'password = "***REDACTED***" ""'),
    ],
)
def test_empty_pair_that_opens_a_longer_value_is_redacted(text, expected):
    # Only a pair that ends the value is an empty credential: a `"""` opener, a
    # pair followed by more text on the same line, and two concatenated pairs
    # each continue the value, so they are redacted as usual instead of being
    # read as `nothing to hide` and left in place.
    assert redact_credentials(text) == expected


@pytest.mark.parametrize(
    "text, key, secret",
    [
        ('password = "s3cret-${DB_PASSWORD}"', "password", "s3cret"),
        ("password = 's3cret-${DB_PASSWORD}'", "password", "s3cret"),
        ("password = s3cret-${DB_PASSWORD}", "password", "s3cret"),
        ("password = ${DB_PASSWORD}-s3cret", "password", "s3cret"),
        ("password = ${?DB_PASSWORD}s3cret", "password", "s3cret"),
        ("password = $${DB_PASSWORD}s3cret", "password", "s3cret"),
        ("client_secret = s3cret${?DB_PASSWORD}", "client_secret", "s3cret"),
        ("access_key = ${DB_PASSWORD}s3cret", "access_key", "s3cret"),
        ("password = ${DB_PASSWORD}${?OTHER}s3cret", "password", "s3cret"),
        ("password = s3cret${DB_PASSWORD}${?OTHER}", "password", "s3cret"),
    ],
)
def test_redacts_literal_secret_next_to_substitution(text, key, secret):
    # A value that mixes literal text with a reference still contains the literal
    # secret, so the complete value is redacted and no `{DB_PASSWORD}` tail is
    # left behind.
    redacted = redact_credentials(text)
    assert secret not in redacted
    assert key in redacted
    assert "REDACTED" in redacted
    assert "DB_PASSWORD" not in redacted
    assert "$" not in redacted
    assert "{" not in redacted


def test_redacts_dollar_value_that_does_not_open_a_substitution():
    # Only the `$` that opens `${...}` is exempt; a `$` anywhere else stays part
    # of the value.
    assert redact_credentials("password = $DB_PASSWORD") == "password = ***REDACTED***"


@pytest.mark.parametrize(
    "text, expected",
    [
        ("password = ${", "password = ${"),
        ("password = ${DB_PASSWORD", "password = ${DB_PASSWORD"),
        ("password = x${y", "password = ***REDACTED***${y"),
        ('password = "x${y', 'password = "***REDACTED***"'),
    ],
)
def test_malformed_substitution_is_not_rewritten_into_a_marker(text, expected):
    # An unclosed substitution is not a secret, so it stays readable instead of
    # having only its `$` replaced; a real value in front of it is still
    # redacted completely.
    assert redact_credentials(text) == expected


def test_session_json_keeps_hocon_substitution_in_last_config(tmp_path):
    # last_config is reloaded on /resume and written out again by /save and
    # /export, so a substitution has to survive the stored round trip.
    manager = SessionManager(tmp_path)
    config = (
        "env {\n  parallelism = 1\n}\n"
        "source {\n  Jdbc {\n"
        '    url = "jdbc:mysql://db:3306/app"\n'
        '    user = "app"\n'
        "    password = ${?MYSQL_PASSWORD}\n"
        "    client_secret = ${DB_CLIENT_SECRET}${?MYSQL_CLIENT_SECRET}\n"
        "  }\n}\n"
    )
    manager.save_session([{"role": "user", "content": [{"text": config}]}], config)
    raw = (manager.sessions_dir / f"{manager.current_session_id}.json").read_text()
    assert json.loads(raw)["last_config"] == config


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

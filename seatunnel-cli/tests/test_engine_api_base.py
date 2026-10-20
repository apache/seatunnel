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

"""Tests for the engine REST base URL the CLI talks to by default.

The default is load-bearing and easy to get wrong, because SeaTunnel exposes
two HTTP ports that answer on different path prefixes: the Jetty HTTP port
(`seatunnel.engine.http.port`, 8080 in the packaged config) serves the v2 REST
API at `/running-jobs`, `/option-rules`, `/submit-job` and `/job-info`, while
the Hazelcast member port 5801 serves the same handlers only under
`/hazelcast/rest/maps` and only when Hazelcast's own REST API is enabled.

A wrong default is silent: every call 404s, the engine reads as unreachable,
and the CLI quietly falls back to offline metadata. These tests pin the port
and the override so that cannot regress unnoticed.
"""

import importlib

from seatunnel_cli import connectors


def _reload_with_env(monkeypatch, value):
    """Reload `connectors` so it re-resolves the base URL from the environment.

    The module reads SEATUNNEL_API_BASE once at import time, which is why the
    production code caches it in a module attribute and why a test has to
    reload rather than just set the variable.
    """
    if value is None:
        monkeypatch.delenv("SEATUNNEL_API_BASE", raising=False)
    else:
        monkeypatch.setenv("SEATUNNEL_API_BASE", value)
    return importlib.reload(connectors)


def test_the_default_is_the_jetty_http_port(monkeypatch):
    try:
        assert _reload_with_env(monkeypatch, None)._ENGINE_API_BASE == "http://localhost:8080"
    finally:
        # Leave the module resolved the way the rest of the session expects.
        monkeypatch.undo()
        importlib.reload(connectors)


def test_the_environment_override_still_wins(monkeypatch):
    # The override is the documented escape hatch for a remote cluster, and for
    # a cluster whose enable-dynamic-port moved the listener off 8080.
    try:
        reloaded = _reload_with_env(monkeypatch, "http://zeta-master:18080")
        assert reloaded._ENGINE_API_BASE == "http://zeta-master:18080"
    finally:
        monkeypatch.undo()
        importlib.reload(connectors)

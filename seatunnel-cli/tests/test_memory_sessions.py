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

"""Tests for session activity ordering."""

from seatunnel_cli.memory import SessionManager


def test_resume_uses_last_active_session_before_creation_order(tmp_path, monkeypatch):
    manager = SessionManager(tmp_path)
    monkeypatch.setattr(
        "seatunnel_cli.memory._now_iso", lambda: "2026-10-01T00:00:00+00:00"
    )
    manager.current_session_id = "20261001_old"
    manager.save_session([])
    monkeypatch.setattr(
        "seatunnel_cli.memory._now_iso", lambda: "2026-10-02T00:00:00+00:00"
    )
    manager.current_session_id = "20261002_new"
    manager.save_session([])
    manager.load_session("20261001_old")
    monkeypatch.setattr(
        "seatunnel_cli.memory._now_iso", lambda: "2026-10-03T00:00:00+00:00"
    )
    manager.save_session([])

    assert manager.get_latest_session_id() == "20261001_old"
    assert [s["session_id"] for s in manager.list_sessions(limit=1)] == ["20261001_old"]
    assert manager.list_sessions()[0]["created_at"] == "2026-10-01T00:00:00+00:00"


def test_latest_session_skips_corrupt_files_and_uses_legacy_creation_time(tmp_path):
    manager = SessionManager(tmp_path)
    (manager.sessions_dir / "zz_corrupt.json").write_text("{invalid")
    (manager.sessions_dir / "legacy.json").write_text(
        '{"session_id":"legacy","created_at":"2026-10-01T00:00:00+00:00"}'
    )
    assert manager.get_latest_session_id() == "legacy"
    assert manager.list_sessions(limit=0) == []


def test_malformed_session_metadata_does_not_block_healthy_resume(tmp_path):
    import json

    manager = SessionManager(tmp_path)
    for key in ("session_id", "created_at", "last_active"):
        for value in (None, 123, []):
            data = {
                "session_id": "invalid",
                "created_at": "2026-10-01",
                "last_active": "2026-10-02",
            }
            data[key] = value
            (
                manager.sessions_dir / f"bad_{key}_{type(value).__name__}.json"
            ).write_text(json.dumps(data))
    manager.current_session_id = "healthy"
    manager.save_session([])
    assert manager.get_latest_session_id() == "healthy"
    assert len(manager.list_sessions()) == 1

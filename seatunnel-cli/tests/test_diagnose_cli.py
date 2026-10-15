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

"""End-to-end tests for ``seatunnel --diagnose <jobId>``.

These drive `run_diagnose` against a real loopback HTTP server so the whole
path is covered: URL construction, the REST call, the rules, redaction and the
exit code. No LLM provider is constructed anywhere here, which is itself part
of what is being asserted -- the flag has to work for a user with no API key.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from rich.console import Console

from seatunnel_cli import cli as cli_module


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.server.paths.append(self.path)
        status, body = self.server.script(self)
        payload = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


@pytest.fixture
def engine(monkeypatch):
    httpd = HTTPServer(("127.0.0.1", 0), _Handler)
    httpd.paths = []
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{httpd.server_port}"
    # `connectors` resolves the base at import time, so patch the module
    # attribute rather than the environment.
    monkeypatch.setattr("seatunnel_cli.connectors._ENGINE_API_BASE", base)
    try:
        yield httpd
    finally:
        httpd.shutdown()
        httpd.server_close()


def _run(job_id="852"):
    console = Console(theme=cli_module.THEME, width=200, record=True)
    code = cli_module.run_diagnose(job_id, console)
    return code, " ".join(console.export_text().split())


def test_a_failed_job_prints_the_error_code_and_exits_nonzero(engine):
    engine.script = lambda h: (
        200,
        json.dumps(
            {
                "jobId": "852",
                "jobName": "mysql-to-doris",
                "jobStatus": "FAILED",
                "errorMsg": (
                    "Caused by: org.apache.seatunnel.common.exception.SeaTunnelRuntimeException: "
                    "ErrorCode:[JDBC-05], ErrorDescription:[Connection failed]"
                ),
            }
        ),
    )

    code, out = _run()

    assert code == 1
    assert "mysql-to-doris" in out
    assert "JDBC-05" in out
    # Both reads are issued, and both are GETs, so the command stays read-only.
    assert engine.paths == ["/job-info/852", "/jobs/checkpoints/852"]


_NOW = 1_760_000_000_000


def _crash_looping_job(name="mysql-to-doris"):
    # `generatedAt` is what the rules use as "now", so pinning it here makes
    # the restore recency deterministic without touching the system clock.
    return json.dumps(
        {
            "jobId": "852",
            "jobName": name,
            "jobStatus": "RUNNING",
            "diagnostics": {
                "generatedAt": _NOW,
                "pipelines": [
                    {
                        "pipelineId": 1,
                        "restoreCount": 5,
                        "maxRestoreCount": 10,
                        "pipelineStatus": "RUNNING",
                        "stateTimestamps": {"RUNNING": _NOW - 20_000},
                    }
                ],
            },
        }
    )


def test_a_crash_looping_job_is_reported_but_exits_zero(engine):
    # The job is alive, so this must not look like a command failure.
    engine.script = lambda h: (200, _crash_looping_job())

    code, out = _run()

    assert code == 0
    assert "restarted 5 times" in out


def test_the_severity_label_reaches_the_terminal(engine):
    # `Finding.render()` prefixes "[warning]", and "warning" is a style name in
    # THEME, so with rich markup enabled the label is consumed as a tag and the
    # severity survives only as colour -- invisible in a pipe, in CI logs and
    # under NO_COLOR, which is where this output usually ends up.
    engine.script = lambda h: (200, _crash_looping_job())

    _, out = _run()

    assert "[warning]" in out


def test_bracketed_engine_text_is_not_swallowed(engine):
    # A SeaTunnel error message is full of lowercase bracketed fields, and
    # those look exactly like style tags.
    engine.script = lambda h: (
        200,
        json.dumps(
            {
                "jobId": "852",
                "jobStatus": "FAILED",
                "errorMsg": "ErrorCode:[JDBC-05], ErrorDescription:[connection refused]",
            }
        ),
    )

    code, out = _run()

    assert code == 1
    assert "[error]" in out
    assert "connection refused" in out


def test_a_path_that_looks_like_a_closing_tag_does_not_crash(engine):
    # "[/data/x.csv]" parses as a closing tag and raises MarkupError, which
    # would crash the command in the one situation it exists for.
    engine.script = lambda h: (
        200,
        json.dumps(
            {
                "jobId": "852",
                "jobStatus": "FAILED",
                "errorMsg": "java.io.IOException: cannot write [/data/x.csv]",
            }
        ),
    )

    code, out = _run()

    assert code == 1
    assert "/data/x.csv" in out


def test_a_bracketed_job_name_is_not_read_as_markup(engine):
    # The job name comes from whoever submitted the job.
    engine.script = lambda h: (200, _crash_looping_job(name="[prod] nightly"))

    _, out = _run()

    assert "[prod] nightly" in out


def test_a_non_numeric_job_id_is_rejected_before_any_request(engine):
    # The engine parses a job id as a decimal long, so "1/2" would be sent as
    # the path /job-info/1/2 and come back as a 404 about a missing job.
    engine.script = lambda h: (200, "{}")

    code, out = _run("1/2")

    assert code == 1
    assert "is not a job id" in out
    assert engine.paths == []


def test_a_healthy_job_says_nothing_to_report(engine):
    engine.script = lambda h: (200, json.dumps({"jobId": "852", "jobStatus": "FINISHED"}))

    code, out = _run()

    assert code == 0
    assert "Nothing to report" in out


def test_an_unknown_job_id_is_reported_from_the_response_the_engine_really_sends(engine):
    # The decisive case, and not the obvious one: `JobInfoService` does not
    # 404 for an id it does not know. When the job is in neither the
    # running-job map nor the finished-job state it falls through to
    # `{"jobId": "<id>"}` with HTTP 200. Treated as a job, that response has
    # no status, no rule matches, and the output used to read "Nothing to
    # report" -- telling someone their typo'd job id is healthy.
    engine.script = lambda h: (200, json.dumps({"jobId": "999999999999999999"}))

    code, out = _run("999999999999999999")

    assert code == 1
    assert "No job 999999999999999999" in out
    assert "Nothing to report" not in out


def test_a_non_object_json_body_does_not_crash(engine):
    # A bare `null` or a list (a proxy, or a future response shape) reaches
    # the same path and must not raise AttributeError.
    for body in ("null", "[]", '"a string"'):
        engine.script = lambda h, b=body: (200, b)

        code, out = _run()

        assert code == 1
        assert "No job 852" in out


def test_a_real_404_is_still_handled(engine):
    # Jetty does not send it for a missing job, but a proxy or a different
    # engine build can, and that branch still has to say something useful.
    engine.script = lambda h: (404, "job not found")

    code, out = _run("999999999999999999")

    assert code == 1
    assert "No job 999999999999999999" in out
    # A 404 is also what a base URL pointing at the member port rather than
    # the Jetty HTTP port looks like, so the message has to offer that way out.
    assert "SEATUNNEL_API_BASE" in out


def test_a_running_job_without_diagnostics_says_the_checks_did_not_run(engine):
    # The engine omits the `diagnostics` block when the master cannot serve
    # it, which leaves the crash-loop and stuck-state rules with no input.
    # Silence there is the same false reassurance as the unknown-id case.
    engine.script = lambda h: (
        200,
        json.dumps({"jobId": "852", "jobStatus": "RUNNING", "metrics": {}}),
    )

    code, out = _run()

    assert code == 0
    assert "Restart counts and state ages were not available" in out
    assert "Nothing to report" not in out


def test_a_finished_job_without_diagnostics_is_not_warned_about(engine):
    # A terminal job legitimately has no diagnostics block: the engine builds
    # it only for a job the master still coordinates. Reporting that as a gap
    # would fire on every finished job.
    engine.script = lambda h: (200, json.dumps({"jobId": "852", "jobStatus": "FINISHED"}))

    code, out = _run()

    assert code == 0
    assert "were not available" not in out
    assert "Nothing to report" in out


def test_the_http_error_body_is_redacted_and_capped(engine):
    # The body is printed, so it needs the same treatment as the findings:
    # it can quote a JDBC URL with a password, and rest.py only bounds it at
    # 8 MiB, which is not a terminal-sized amount of text.
    secret = "jdbc:mysql://db:3306/app?user=root&password=sup3rs3cret"
    engine.script = lambda h: (500, f"submit failed for {secret} " + ("x" * 5000))

    code, out = _run()

    assert code == 1
    assert "sup3rs3cret" not in out
    assert "chars total" in out
    assert len(out) < 5000


def test_other_http_errors_show_the_engine_body(engine):
    engine.script = lambda h: (500, "ErrorCode:[API-02], ErrorDescription:[master is down]")

    code, out = _run()

    assert code == 1
    # The bracketed description is the useful half of the body; asserting only
    # on "API-02" is what let the markup bug through in the first place.
    assert "ErrorDescription:[master is down]" in out


def test_an_unreachable_engine_names_the_override_env_var(monkeypatch):
    # Nothing is listening on port 1, so this is the real "engine is down"
    # path, not a simulated one.
    monkeypatch.setattr("seatunnel_cli.connectors._ENGINE_API_BASE", "http://127.0.0.1:1")

    code, out = _run()

    assert code == 1
    assert "SEATUNNEL_API_BASE" in out


def test_credentials_in_the_error_message_are_redacted(engine):
    # A JDBC failure quotes the URL it tried, password included, and this
    # output routinely gets pasted into a bug report.
    engine.script = lambda h: (
        200,
        json.dumps(
            {
                "jobId": "852",
                "jobStatus": "FAILED",
                "errorMsg": (
                    "Caused by: java.sql.SQLException: Could not connect to "
                    "jdbc:mysql://db:3306/app?user=root&password=sup3rs3cret"
                ),
            }
        ),
    )

    code, out = _run()

    assert code == 1
    assert "sup3rs3cret" not in out


def _routed(job_info, checkpoints=None, checkpoint_status=200):
    """Serve job-info and the checkpoint overview from one fake engine."""

    def script(handler):
        if handler.path.startswith("/jobs/checkpoints/"):
            if checkpoints is None:
                return checkpoint_status, "no checkpoint data"
            return checkpoint_status, json.dumps(checkpoints)
        return 200, json.dumps(job_info)

    return script


def _slow_checkpoint_history(count=8):
    # Newest first, matching the engine. Recent checkpoints take 40s, older
    # ones took 4s, so the job has visibly got slower.
    now = 1_760_000_000_000
    entries = []
    for index in range(count):
        duration = 40_000 if index < count // 2 else 4_000
        entries.append(
            {
                "pipelineId": 1,
                "checkpoint": {
                    "checkpointId": 500 - index,
                    "status": "COMPLETED",
                    "triggerTimestamp": now - index * 600_000,
                    "completedTimestamp": now - index * 600_000 + duration,
                    "durationMillis": duration,
                    "stateSize": 4096,
                },
            }
        )
    return entries


def test_a_slowing_cdc_job_is_explained_even_though_rows_still_flow(engine):
    # This is the case job-info alone cannot answer: RUNNING, both counters
    # moving, no restores. Without the checkpoint read the only output would
    # be "Running for ...", which restates the symptom.
    engine.script = _routed(
        {
            "jobId": "852",
            "jobName": "mysql-cdc-to-doris",
            "jobStatus": "RUNNING",
            "metrics": {"SourceReceivedCount": "184203311", "SinkWriteCount": "184201902"},
        },
        {"jobId": "852", "pipelines": [{"pipelineId": 1, "history": _slow_checkpoint_history()}]},
    )

    code, out = _run()

    assert code == 0
    assert "getting slower" in out
    assert "40.0s" in out and "4.0s" in out


def test_a_missing_checkpoint_endpoint_does_not_break_the_diagnosis(engine):
    # An older engine has no such endpoint. The job diagnosis must survive it,
    # because losing the whole answer to gain one optional rule is a bad trade.
    engine.script = _routed(
        {
            "jobId": "852",
            "jobStatus": "FAILED",
            "errorMsg": "ErrorCode:[JDBC-05], ErrorDescription:[Connection failed]",
        },
        checkpoint_status=404,
    )

    code, out = _run()

    assert code == 1
    assert "JDBC-05" in out
    # No notice for a terminal job: a finished or failed job legitimately has
    # no checkpoint overview retained, so reporting its absence would fire on
    # every one of them. The live-job case is covered below.
    assert "No checkpoint history" not in out


def test_a_healthy_job_with_no_checkpoint_data_does_not_look_fully_checked(engine):
    # The combination that matters: the job-level rules ran and found nothing,
    # while the checkpoint read failed. Without the notice this prints only
    # "Nothing to report", which reads as "the slowdown checks ran and found
    # nothing". The job carries a diagnostics block so that this test isolates
    # the checkpoint gap rather than also tripping the job-level one.
    engine.script = _routed(
        {
            "jobId": "852",
            "jobStatus": "RUNNING",
            "diagnostics": {"generatedAt": _NOW, "stateTimestamps": {"RUNNING": _NOW - 5_000}},
        },
        checkpoint_status=404,
    )

    code, out = _run()

    assert code == 0
    assert "No checkpoint history for this job" in out
    # The notice replaces "Nothing to report" rather than sitting next to it:
    # the slowdown half never ran, so a clean bill of health is not something
    # this output can give.
    assert "Nothing to report" not in out


def test_a_cluster_with_the_monitor_off_is_reported_not_passed_over(engine):
    # The shape that actually occurs: with the monitor service off, or with
    # nothing recorded for the job yet, CheckpointMonitorRestService answers
    # 200 with only {"jobId": ...} and no `pipelines`. That is not an
    # exception, so an except-only notice would never print and the output
    # would end on "Nothing to report" with the slowdown half never run.
    engine.script = _routed(
        {
            "jobId": "852",
            "jobStatus": "RUNNING",
            "diagnostics": {"generatedAt": _NOW, "stateTimestamps": {"RUNNING": _NOW - 5_000}},
        },
        checkpoints={"jobId": "852"},
    )

    code, out = _run()

    assert code == 0
    assert "No checkpoint history for this job" in out
    assert "Nothing to report" not in out


def test_an_unparsable_checkpoint_response_does_not_break_the_diagnosis(engine):
    engine.script = _routed(
        {
            "jobId": "852",
            "jobStatus": "FAILED",
            "errorMsg": "ErrorCode:[JDBC-05], ErrorDescription:[Connection failed]",
        },
        checkpoint_status=200,
    )

    code, out = _run()

    assert code == 1
    assert "JDBC-05" in out


def test_a_checkpoint_error_outranks_a_job_level_info_line(engine):
    # The two rule sets are merged and sorted together. Sorting them
    # separately would print "Running for 72h" above a stalled checkpoint.
    now = 1_760_000_000_000
    engine.script = _routed(
        {
            "jobId": "852",
            "jobStatus": "RUNNING",
            "diagnostics": {"stateTimestamps": {"RUNNING": now - 72 * 3600 * 1000}},
        },
        {
            "jobId": "852",
            "pipelines": [
                {
                    "pipelineId": 1,
                    "inProgress": [
                        {
                            "checkpointId": 91,
                            "triggerTimestamp": now - 1800 * 1000,
                            "acknowledged": 2,
                            "total": 6,
                        }
                    ],
                }
            ],
        },
    )

    code, out = _run()

    assert code == 0
    lines = [line for line in out.splitlines() if "[error]" in line or "[info]" in line]
    assert "[error]" in lines[0]
    assert "2 of 6" in out


def test_the_severity_label_is_printed_and_not_eaten_as_markup(engine):
    # "error" is a style name in THEME, so rich would treat the "[error]"
    # prefix as a style tag and drop it, leaving severity carried by colour
    # alone -- lost in a pipe, in CI, or under NO_COLOR.
    engine.script = _routed(
        {
            "jobId": "852",
            "jobStatus": "FAILED",
            "errorMsg": "ErrorCode:[JDBC-05], ErrorDescription:[Connection failed]",
        },
        checkpoint_status=404,
    )
    console = Console(theme=cli_module.THEME, width=200, record=True, no_color=True)

    code = cli_module.run_diagnose("852", console)
    out = console.export_text()

    assert code == 1
    assert "[error]" in out


def test_brackets_in_an_engine_message_cannot_corrupt_the_output(engine):
    # The checkpoint failure reason is engine text passed straight through to
    # the terminal, and a Java message routinely contains bracketed text. The
    # error headline would not do here: it keeps only the code and
    # description, so brackets never reach the output through that path.
    engine.script = _routed(
        {"jobId": "852", "jobStatus": "RUNNING"},
        {
            "jobId": "852",
            "pipelines": [
                {
                    "pipelineId": 1,
                    "counts": {"triggered": 10, "completed": 6, "failed": 4},
                    "latestFailed": {
                        "failureReason": "expired at Table[/bold] row[nope] idx[0]"
                    },
                }
            ],
        },
    )

    code, out = _run()

    # Rich wraps the line, so compare with whitespace normalised.
    flat = " ".join(out.split())
    assert code == 0
    assert "Table[/bold] row[nope] idx[0]" in flat

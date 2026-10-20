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

"""Tests for the rule-based job diagnosis.

The job-info payloads here are shaped the way the engine actually sends them,
which is not the way one would guess: every metric leaf is a *string* because
`BaseService.metricsToJsonObject` calls `toString()` on it, the job-wide row
counts are `SourceReceivedCount`/`SinkWriteCount` while the `Table`-prefixed
names are objects keyed by table, and `stateTimestamps` is a state-name map
rather than an ordered list. Those three details are the ones a diagnosis
built on assumptions would get wrong, so they are pinned here.
"""

from seatunnel_cli.diagnostics.jobs import (
    CRASH_LOOP_RECENT_SECONDS,
    CRASH_LOOP_RESTORES,
    STUCK_SECONDS,
    diagnose_job,
)

NOW = 1_760_000_000_000  # fixed clock, so the stuck rule is not time-dependent

# Long enough for the progress rules to apply at all: a zero row count on a
# job that started seconds ago is the expected reading, not a symptom.
RUNNING_LONG_ENOUGH = {"RUNNING": NOW - (STUCK_SECONDS + 60) * 1000}


def _rules(findings):
    return [f.rule for f in findings]


def _by_rule(findings, rule):
    return next(f for f in findings if f.rule == rule)


def test_failed_job_leads_with_the_parsed_error_code():
    findings = diagnose_job(
        {
            "jobStatus": "FAILED",
            "errorMsg": (
                "java.util.concurrent.CompletionException: wrapper\n"
                "Caused by: org.apache.seatunnel.common.exception.SeaTunnelRuntimeException: "
                "ErrorCode:[JDBC-05], ErrorDescription:[Connection failed]\n"
                "Caused by: java.sql.SQLException: Access denied for user 'bench'\n"
            ),
        },
        now_ms=NOW,
    )

    assert "job_failed" in _rules(findings)
    assert "JDBC-05" in _by_rule(findings, "job_failed").detail


def test_failed_with_no_error_message_says_so_instead_of_staying_silent():
    # Silence here would be the worst outcome: the user would think the tool
    # had nothing to say about a job that definitely broke.
    findings = diagnose_job({"jobStatus": "FAILED", "errorMsg": "   "}, now_ms=NOW)

    assert _rules(findings) == ["failed_without_error"]


def test_repeated_restores_with_a_young_attempt_are_reported_as_a_crash_loop():
    findings = diagnose_job(
        {
            "jobStatus": "RUNNING",
            "diagnostics": {
                "pipelines": [
                    {
                        "pipelineId": 1,
                        "restoreCount": 4,
                        "maxRestoreCount": 10,
                        "pipelineStatus": "RUNNING",
                        # Restored 30s ago: the loop is happening now.
                        "stateTimestamps": {"RUNNING": NOW - 30_000},
                    },
                ],
            },
        },
        now_ms=NOW,
    )

    loop = _by_rule(findings, "pipeline_crash_loop")
    assert loop.severity == "warning"
    assert "restoreCount=4/10" in loop.evidence
    assert "30s old" in loop.detail


def test_old_restores_on_a_steady_pipeline_are_history_not_a_crash_loop():
    # `restoreCount` counts restores since submission, so a streaming job that
    # recovered from two transient faults weeks ago carries the same count as
    # one restarting every minute. Reporting the first as "making no progress"
    # is simply false, and it is the reading a long-lived job most often has.
    findings = diagnose_job(
        {
            "jobStatus": "RUNNING",
            "diagnostics": {
                "pipelines": [
                    {
                        "pipelineId": 1,
                        "restoreCount": 2,
                        "maxRestoreCount": 10,
                        "pipelineStatus": "RUNNING",
                        "stateTimestamps": {"RUNNING": NOW - 14 * 86400 * 1000},
                    },
                ],
            },
        },
        now_ms=NOW,
    )

    assert "pipeline_crash_loop" not in _rules(findings)
    note = _by_rule(findings, "pipeline_restored_before")
    assert note.severity == "info"
    assert "has been running since" in note.detail
    assert "no progress" not in note.detail


def test_a_pipeline_not_running_with_restores_behind_it_is_a_crash_loop():
    # Mid-restart is the one moment the loop is directly observable.
    findings = diagnose_job(
        {
            "jobStatus": "RUNNING",
            "diagnostics": {
                "pipelines": [
                    {
                        "pipelineId": 1,
                        "restoreCount": 5,
                        "maxRestoreCount": 10,
                        "pipelineStatus": "SCHEDULED",
                        "stateTimestamps": {"SCHEDULED": NOW - 1000},
                    },
                ],
            },
        },
        now_ms=NOW,
    )

    loop = _by_rule(findings, "pipeline_crash_loop")
    assert loop.severity == "warning"
    assert "SCHEDULED rather than RUNNING" in loop.detail


def test_restores_with_no_timing_evidence_are_reported_as_a_bare_count():
    # An older engine, or a master that has just taken over, can send the
    # counter without pipeline state timestamps. Without them there is no
    # evidence of a loop, so the count is reported as a count.
    findings = diagnose_job(
        {
            "jobStatus": "RUNNING",
            "diagnostics": {
                "pipelines": [{"pipelineId": 1, "restoreCount": 4, "maxRestoreCount": 10}],
            },
        },
        now_ms=NOW,
    )

    note = _by_rule(findings, "pipeline_restored_before")
    assert note.severity == "info"
    assert "not visible in this response" in note.detail


def _pipeline(status, restores=3, limit=10, **extra):
    p = {
        "pipelineId": 1,
        "restoreCount": restores,
        "maxRestoreCount": limit,
        "pipelineStatus": status,
        "stateTimestamps": {status: NOW - 1000},
    }
    p.update(extra)
    return p


def test_finished_and_cancelled_pipelines_are_not_called_restarting():
    # A restorable pipeline is in an end state for an instant only:
    # prepareRestorePipeline increments the counter and immediately resets
    # back toward CREATED. So a pipeline *observed* in an end state is over,
    # and a restore count on it is history. In a multi-pipeline job this is
    # the normal reading for a pipeline that finished after two restores.
    for status in ("FINISHED", "CANCELED"):
        findings = diagnose_job(
            {"jobStatus": "RUNNING", "diagnostics": {"pipelines": [_pipeline(status)]}},
            now_ms=NOW,
        )

        assert findings == [], status


def test_an_end_state_pipeline_at_its_restore_limit_is_not_an_error_about_the_future():
    # "The next failure ends the job" is meaningless for a pipeline that has
    # already finished, and it was previously reported as an error.
    for status in ("FINISHED", "CANCELED"):
        findings = diagnose_job(
            {
                "jobStatus": "RUNNING",
                "diagnostics": {"pipelines": [_pipeline(status, restores=10, limit=10)]},
            },
            now_ms=NOW,
        )

        assert findings == [], status


def test_a_failed_pipeline_is_reported_in_the_past_tense_only_when_exhausted():
    exhausted = diagnose_job(
        {
            "jobStatus": "FAILING",
            "diagnostics": {"pipelines": [_pipeline("FAILED", restores=10, limit=10)]},
        },
        now_ms=NOW,
    )
    spent = _by_rule(exhausted, "pipeline_restore_exhausted")
    assert "used all 10 of its restores and then failed" in spent.detail
    assert "next failure" not in spent.detail

    # Below the limit a failure was not restorable; the job-level failure
    # finding covers it, so there is nothing to add here.
    not_exhausted = diagnose_job(
        {
            "jobStatus": "FAILING",
            "diagnostics": {"pipelines": [_pipeline("FAILED", restores=3, limit=10)]},
        },
        now_ms=NOW,
    )
    assert not_exhausted == []


def test_a_tearing_down_pipeline_reports_restores_as_history():
    for status in ("CANCELING", "FAILING"):
        findings = diagnose_job(
            {"jobStatus": "CANCELING", "diagnostics": {"pipelines": [_pipeline(status)]}},
            now_ms=NOW,
        )

        note = _by_rule(findings, "pipeline_restored_before")
        assert note.severity == "info", status
        assert "no progress" not in note.detail


def test_a_finished_pipeline_next_to_a_running_one_is_not_reported():
    # The multi-pipeline case the end-state bug showed up in: one pipeline
    # finishing must not make the whole job look like it is crash-looping.
    findings = diagnose_job(
        {
            "jobStatus": "RUNNING",
            "diagnostics": {
                "pipelines": [
                    _pipeline("FINISHED", restores=4),
                    {
                        "pipelineId": 2,
                        "restoreCount": 4,
                        "maxRestoreCount": 10,
                        "pipelineStatus": "RUNNING",
                        "stateTimestamps": {"RUNNING": NOW - 30_000},
                    },
                ]
            },
        },
        now_ms=NOW,
    )

    rules = _rules(findings)
    assert rules.count("pipeline_crash_loop") == 1
    assert "Pipeline 2" in _by_rule(findings, "pipeline_crash_loop").detail


def test_a_pipeline_between_attempts_is_reported_as_such():
    findings = diagnose_job(
        {"jobStatus": "RUNNING", "diagnostics": {"pipelines": [_pipeline("DEPLOYING")]}},
        now_ms=NOW,
    )

    loop = _by_rule(findings, "pipeline_crash_loop")
    assert loop.severity == "warning"
    assert "between attempts" in loop.detail


def test_a_pipeline_restored_just_under_the_recency_window_still_warns():
    findings = diagnose_job(
        {
            "jobStatus": "RUNNING",
            "diagnostics": {
                "pipelines": [
                    {
                        "pipelineId": 1,
                        "restoreCount": 3,
                        "maxRestoreCount": 10,
                        "pipelineStatus": "RUNNING",
                        "stateTimestamps": {
                            "RUNNING": NOW - (CRASH_LOOP_RECENT_SECONDS - 1) * 1000
                        },
                    },
                ],
            },
        },
        now_ms=NOW,
    )

    assert _by_rule(findings, "pipeline_crash_loop").severity == "warning"


def test_a_pipeline_at_its_restore_limit_is_an_error_not_a_warning():
    findings = diagnose_job(
        {
            "jobStatus": "RUNNING",
            "diagnostics": {
                "pipelines": [
                    {"pipelineId": 2, "restoreCount": 3, "maxRestoreCount": 3},
                ],
            },
        },
        now_ms=NOW,
    )

    exhausted = _by_rule(findings, "pipeline_restore_exhausted")
    assert exhausted.severity == "error"


def test_a_single_restore_is_not_flagged():
    # One restore is ordinary recovery from a transient fault; flagging it
    # would make healthy clusters look sick.
    findings = diagnose_job(
        {
            "jobStatus": "RUNNING",
            "diagnostics": {
                "pipelines": [{"pipelineId": 1, "restoreCount": CRASH_LOOP_RESTORES - 1}],
            },
        },
        now_ms=NOW,
    )

    assert findings == []


def test_long_scheduled_state_is_reported_as_stuck():
    entered = NOW - (STUCK_SECONDS + 60) * 1000
    findings = diagnose_job(
        {
            "jobStatus": "SCHEDULED",
            "diagnostics": {"stateTimestamps": {"CREATED": entered - 5000, "SCHEDULED": entered}},
        },
        now_ms=NOW,
    )

    stuck = _by_rule(findings, "stuck_before_running")
    assert "SCHEDULED for 3m00s" in stuck.detail
    assert "slots" in stuck.detail


def test_the_duration_comes_from_the_current_state_not_an_older_one():
    # A restored job re-enters an earlier state, so the map holds entries for
    # several states. Reading the wrong one -- the last key, or the oldest --
    # reports a duration off by days.
    findings = diagnose_job(
        {
            "jobStatus": "SCHEDULED",
            "diagnostics": {
                "stateTimestamps": {
                    "SCHEDULED": NOW - (STUCK_SECONDS + 1) * 1000,
                    "CREATED": NOW - 10 * 24 * 3600 * 1000,
                }
            },
        },
        now_ms=NOW,
    )

    assert "2m01s" in _by_rule(findings, "stuck_before_running").detail


def test_a_state_with_no_entry_of_its_own_falls_back_to_the_newest_entry():
    # PENDING has no timestamp here, so the best available answer is the most
    # recent transition the engine did record.
    findings = diagnose_job(
        {
            "jobStatus": "PENDING",
            "diagnostics": {
                "stateTimestamps": {
                    "CREATED": NOW - 10 * 24 * 3600 * 1000,
                    "INITIALIZING": NOW - (STUCK_SECONDS + 1) * 1000,
                }
            },
        },
        now_ms=NOW,
    )

    assert "2m01s" in _by_rule(findings, "stuck_before_running").detail


def test_a_job_stuck_tearing_down_is_not_blamed_on_cluster_slots():
    # CANCELING forever is a task ignoring the stop request, so pointing the
    # user at free slots would send them to the wrong place entirely.
    findings = diagnose_job(
        {
            "jobStatus": "CANCELING",
            "diagnostics": {
                "stateTimestamps": {"CANCELING": NOW - (STUCK_SECONDS + 1) * 1000}
            },
        },
        now_ms=NOW,
    )

    stuck = _by_rule(findings, "stuck_tearing_down")
    assert "slots" not in stuck.detail
    assert "stop request" in stuck.detail


def test_a_long_savepoint_is_not_described_as_an_unresponsive_task():
    # A savepoint writes the whole job state, so minutes can be exactly what
    # the operator asked for. "Not responding to the stop request" would send
    # someone hunting a hung task over a job doing its job.
    findings = diagnose_job(
        {
            "jobStatus": "DOING_SAVEPOINT",
            "diagnostics": {
                "stateTimestamps": {"DOING_SAVEPOINT": NOW - (STUCK_SECONDS + 1) * 1000}
            },
        },
        now_ms=NOW,
    )

    savepoint = _by_rule(findings, "savepoint_in_progress")
    assert "stop request" not in savepoint.detail
    assert "can be normal" in savepoint.detail


def test_the_masters_generated_at_is_preferred_over_the_local_clock():
    # Every timestamp compared here was written by the master, so mixing in a
    # local clock that is minutes off invents stuck jobs (or hides them). With
    # no `now_ms` injected, `generatedAt` is the right "now".
    job_info = {
        "jobStatus": "SCHEDULED",
        "diagnostics": {
            "generatedAt": NOW,
            "stateTimestamps": {"SCHEDULED": NOW - (STUCK_SECONDS + 60) * 1000},
        },
    }

    assert "3m00s" in _by_rule(diagnose_job(job_info), "stuck_before_running").detail


def test_a_missing_generated_at_falls_back_to_the_local_clock():
    # An older engine sends no diagnostics block at all; the rules that do not
    # need it must still work, so the fallback has to stay.
    job_info = {
        "jobStatus": "SCHEDULED",
        "diagnostics": {"stateTimestamps": {"SCHEDULED": 1_000}},
    }

    assert "stuck_before_running" in _rules(diagnose_job(job_info))


def test_a_freshly_scheduled_job_is_not_called_stuck():
    findings = diagnose_job(
        {
            "jobStatus": "SCHEDULED",
            "diagnostics": {"stateTimestamps": {"SCHEDULED": NOW - 3000}},
        },
        now_ms=NOW,
    )

    assert findings == []


def test_a_long_running_job_is_information_not_a_warning():
    findings = diagnose_job(
        {
            "jobStatus": "RUNNING",
            "diagnostics": {"stateTimestamps": {"RUNNING": NOW - 7200 * 1000}},
        },
        now_ms=NOW,
    )

    running = _by_rule(findings, "running_duration")
    assert running.severity == "info"
    assert "2h00m" in running.detail


def test_a_finished_job_is_not_measured_for_stuckness():
    # FINISHED is terminal, so the age of the state is just how long ago the
    # job ended and says nothing about health.
    findings = diagnose_job(
        {
            "jobStatus": "FINISHED",
            "diagnostics": {"stateTimestamps": {"FINISHED": NOW - 30 * 86400 * 1000}},
        },
        now_ms=NOW,
    )

    assert findings == []


def test_zero_rows_read_is_reported_from_string_valued_metrics():
    # The engine stringifies every metric leaf, so an int comparison on the
    # raw value would never match.
    findings = diagnose_job(
        {
            "jobStatus": "RUNNING",
            "metrics": {"SourceReceivedCount": "0", "SinkWriteCount": "0"},
            "diagnostics": {"stateTimestamps": RUNNING_LONG_ENOUGH},
        },
        now_ms=NOW,
    )

    assert _by_rule(findings, "no_rows_read").severity == "warning"


def test_a_freshly_running_job_that_has_read_nothing_yet_is_not_flagged():
    # A zero counter one second into RUNNING is what every job looks like
    # before its first batch arrives. Warning here would mean every job is
    # reported as broken for its first two minutes.
    findings = diagnose_job(
        {
            "jobStatus": "RUNNING",
            "metrics": {"SourceReceivedCount": "0", "SinkWriteCount": "0"},
            "diagnostics": {"stateTimestamps": {"RUNNING": NOW - 1000}},
        },
        now_ms=NOW,
    )

    assert findings == []


def test_progress_rules_stay_silent_without_a_running_timestamp():
    # No RUNNING entry means the job's uptime is unknown, and the difference
    # between "read nothing for an hour" and "started a second ago" is the
    # whole finding.
    findings = diagnose_job(
        {
            "jobStatus": "RUNNING",
            "metrics": {"SourceReceivedCount": "0", "SinkWriteCount": "0"},
            "diagnostics": {"stateTimestamps": {"CREATED": NOW - 86400 * 1000}},
        },
        now_ms=NOW,
    )

    assert "no_rows_read" not in _rules(findings)


def test_an_idle_streaming_source_is_a_note_not_a_warning():
    # A quiet topic or a CDC job waiting for its first change event reads
    # nothing and is perfectly healthy, so for a streaming job this cannot be
    # asserted as a fault.
    findings = diagnose_job(
        {
            "jobStatus": "RUNNING",
            "envOptions": {"job.mode": "STREAMING"},
            "metrics": {"SourceReceivedCount": "0", "SinkWriteCount": "0"},
            "diagnostics": {"stateTimestamps": RUNNING_LONG_ENOUGH},
        },
        now_ms=NOW,
    )

    idle = _by_rule(findings, "no_rows_read")
    assert idle.severity == "info"
    assert "normal" in idle.detail


def test_a_batch_source_reading_nothing_is_still_a_warning():
    findings = diagnose_job(
        {
            "jobStatus": "RUNNING",
            "envOptions": {"job.mode": "BATCH"},
            "metrics": {"SourceReceivedCount": "0", "SinkWriteCount": "0"},
            "diagnostics": {"stateTimestamps": RUNNING_LONG_ENOUGH},
        },
        now_ms=NOW,
    )

    assert _by_rule(findings, "no_rows_read").severity == "warning"


def test_rows_read_but_none_written_points_between_source_and_sink():
    findings = diagnose_job(
        {
            "jobStatus": "RUNNING",
            "metrics": {"SourceReceivedCount": "1500", "SinkWriteCount": "0"},
            "diagnostics": {"stateTimestamps": RUNNING_LONG_ENOUGH},
        },
        now_ms=NOW,
    )

    dropped = _by_rule(findings, "rows_read_none_written")
    assert "1500" in dropped.detail


def test_healthy_throughput_produces_no_progress_finding():
    findings = diagnose_job(
        {
            "jobStatus": "RUNNING",
            "metrics": {"SourceReceivedCount": "1500", "SinkWriteCount": "1500"},
            "diagnostics": {"stateTimestamps": RUNNING_LONG_ENOUGH},
        },
        now_ms=NOW,
    )

    assert _rules(findings) == ["running_duration"]


def test_table_scoped_metrics_are_not_mistaken_for_scalars():
    # "TableSourceReceivedCount" is an object keyed by table name. Reading it
    # as a count would make `int(...)` fail or, worse, coerce to something
    # meaningless -- either way a false "no rows read" on a healthy job.
    findings = diagnose_job(
        {
            "jobStatus": "RUNNING",
            "metrics": {
                "TableSourceReceivedCount": {"db.public.users": "1500"},
                "TableSinkWriteCount": {"db.public.users": "1500"},
            },
            "diagnostics": {"stateTimestamps": RUNNING_LONG_ENOUGH},
        },
        now_ms=NOW,
    )

    assert _rules(findings) == ["running_duration"]


def test_findings_are_ordered_worst_first():
    entered = NOW - (STUCK_SECONDS + 10) * 1000
    findings = diagnose_job(
        {
            "jobStatus": "FAILED",
            "errorMsg": "ErrorCode:[JDBC-05], ErrorDescription:[Connection failed]",
            "diagnostics": {
                "pipelines": [{"pipelineId": 1, "restoreCount": 2, "maxRestoreCount": 3}],
                "stateTimestamps": {"FAILED": entered},
            },
        },
        now_ms=NOW,
    )

    severities = [f.severity for f in findings]
    assert severities == sorted(severities, key=["error", "warning", "info"].index)
    assert findings[0].rule == "job_failed"


def test_malformed_payloads_yield_no_findings_rather_than_raising():
    # Everything here is a shape the engine should never send, but a crash
    # while diagnosing is strictly worse than saying nothing.
    for payload in (
        None,
        [],
        "FAILED",
        {},
        {"jobStatus": None},
        {"jobStatus": "RUNNING", "diagnostics": "unavailable"},
        {"jobStatus": "RUNNING", "diagnostics": {"pipelines": "none"}},
        {"jobStatus": "RUNNING", "diagnostics": {"pipelines": [None, 7]}},
        {"jobStatus": "RUNNING", "diagnostics": {"stateTimestamps": []}},
        {"jobStatus": "RUNNING", "diagnostics": {"stateTimestamps": {"RUNNING": "soon"}}},
        {"jobStatus": "RUNNING", "metrics": None},
        {"jobStatus": "RUNNING", "metrics": {"SourceReceivedCount": "n/a"}},
    ):
        assert diagnose_job(payload, now_ms=NOW) == []


def test_restore_count_of_true_is_not_counted_as_one():
    # bool is a subclass of int in Python, so an unguarded int() would turn a
    # stray `true` into a restore count.
    findings = diagnose_job(
        {
            "jobStatus": "RUNNING",
            "diagnostics": {"pipelines": [{"pipelineId": 1, "restoreCount": True}]},
        },
        now_ms=NOW,
    )

    assert findings == []

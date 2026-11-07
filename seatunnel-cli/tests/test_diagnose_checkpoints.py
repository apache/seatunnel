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

"""Tests for the checkpoint-history rules.

The payloads are shaped the way `/jobs/checkpoints/{jobId}` actually sends
them, and three of those details are what a diagnosis built on assumptions
would get wrong:

* `history` is **newest first**, so a trend read in array order is inverted;
* failed and canceled checkpoints share the array but have **no**
  `durationMillis` (the field is omitted when null);
* their `stateSize` is a primitive `long`, so it arrives as a plausible `0`
  rather than being absent, which would make growing state look like
  shrinking state.

Each of those has a test of its own below, because all three fail silently --
they produce a confident finding that is simply backwards.
"""

import time

from seatunnel_cli.diagnostics.checkpoints import (
    DURATION_FLOOR_MS,
    MIN_TREND_SAMPLES,
    MIN_TRIGGERS_NEVER_COMPLETED,
    STALLED_CHECKPOINT_SECONDS,
    diagnose_checkpoints,
)

NOW = 1_760_000_000_000


def _rules(findings):
    return [f.rule for f in findings]


def _by_rule(findings, rule):
    return next(f for f in findings if f.rule == rule)


def _history(durations, *, state_sizes=None, interval_ms=600_000, status="COMPLETED"):
    """Build a newest-first history array.

    ``durations`` is given newest first, matching the wire format, so a test
    that wants "recently slow" puts the large values at the front.
    """
    entries = []
    for index, duration in enumerate(durations):
        checkpoint = {
            "checkpointId": 1000 - index,
            "checkpointType": "CHECKPOINT_TYPE",
            "status": status,
            # Newest first, so each successive entry is older.
            "triggerTimestamp": NOW - index * interval_ms,
            "stateSize": (state_sizes[index] if state_sizes else 1024),
        }
        if duration is not None:
            checkpoint["durationMillis"] = duration
            checkpoint["completedTimestamp"] = checkpoint["triggerTimestamp"] + duration
        entries.append({"pipelineId": 1, "checkpoint": checkpoint})
    return entries


def _overview(pipeline):
    return {"jobId": "852", "updatedAt": NOW, "pipelines": [pipeline]}


def test_checkpoints_getting_slower_are_reported():
    findings = diagnose_checkpoints(
        _overview({"pipelineId": 1, "history": _history([30_000] * 4 + [6_000] * 4)}),
        now_ms=NOW,
    )

    growing = _by_rule(findings, "checkpoint_duration_growing")
    assert growing.severity == "warning"
    assert "30.0s" in growing.detail and "6.0s" in growing.detail


def test_checkpoints_getting_faster_are_not_reported_as_slower():
    # This is the ordering trap. `history` is newest first, so these durations
    # describe a job that *recovered*: it used to take 30s and now takes 6s.
    # Reading the array as oldest-first would report a 5x slowdown.
    findings = diagnose_checkpoints(
        _overview({"pipelineId": 1, "history": _history([6_000] * 4 + [30_000] * 4)}),
        now_ms=NOW,
    )

    assert "checkpoint_duration_growing" not in _rules(findings)


def test_failed_checkpoints_do_not_poison_the_duration_trend():
    # Failed entries carry no durationMillis at all. Treating the missing
    # field as a zero would halve the recent median and hide a real slowdown.
    history = _history([30_000, None, 30_000, None, 30_000, 30_000, 6_000, 6_000, 6_000, 6_000])
    for entry in history:
        if "durationMillis" not in entry["checkpoint"]:
            entry["checkpoint"]["status"] = "FAILED"
            entry["checkpoint"]["failureReason"] = "checkpoint expired"

    findings = diagnose_checkpoints(_overview({"pipelineId": 1, "history": history}), now_ms=NOW)

    assert "checkpoint_duration_growing" in _rules(findings)


def test_failed_checkpoints_zero_state_size_does_not_invert_the_state_trend():
    # stateSize is a primitive long on the engine side, so a failed checkpoint
    # reports a convincing 0 rather than omitting the field. Counted in, those
    # zeroes would drag the recent median down and turn growing state into
    # apparently shrinking state.
    sizes = [50 << 20] * 4 + [10 << 20] * 4
    history = _history([30_000] * 8, state_sizes=sizes)
    # Two recent failures, which is exactly when this is most likely to bite.
    for entry in history[:2]:
        entry["checkpoint"]["status"] = "FAILED"
        entry["checkpoint"]["stateSize"] = 0
        del entry["checkpoint"]["durationMillis"]

    findings = diagnose_checkpoints(_overview({"pipelineId": 1, "history": history}), now_ms=NOW)

    assert "checkpoint_state_growing" in _rules(findings)


def test_a_doubling_of_a_trivial_duration_is_not_reported():
    # 8ms to 20ms is a doubling and is not why anything is lagging.
    findings = diagnose_checkpoints(
        _overview(
            {"pipelineId": 1, "history": _history([20] * 4 + [8] * 4, interval_ms=10_000)}
        ),
        now_ms=NOW,
    )

    assert "checkpoint_duration_growing" not in _rules(findings)
    assert DURATION_FLOOR_MS > 20


def test_too_few_checkpoints_yields_no_trend():
    findings = diagnose_checkpoints(
        _overview(
            {"pipelineId": 1, "history": _history([30_000, 30_000, 6_000, 6_000])},
        ),
        now_ms=NOW,
    )

    assert MIN_TREND_SAMPLES > 4
    assert "checkpoint_duration_growing" not in _rules(findings)


def test_a_job_that_spends_its_time_checkpointing_is_reported_without_a_baseline():
    # Durations are flat, so the trend rule stays silent -- this is the job
    # that degraded before the retained window began and has been slow since.
    findings = diagnose_checkpoints(
        _overview(
            {"pipelineId": 1, "history": _history([9_000] * 8, interval_ms=10_000)},
        ),
        now_ms=NOW,
    )

    assert "checkpoint_duration_growing" not in _rules(findings)
    bound = _by_rule(findings, "checkpoint_bound")
    assert "9.0s" in bound.detail and "10.0s" in bound.detail


def test_a_job_that_became_checkpoint_bound_recently_is_still_reported():
    # Healthy for the older half (4s of a 60s interval), checkpoint-bound in
    # the recent half (48s of 60s). A median over the whole window is 26s,
    # which is under the threshold, so taking it would stay silent exactly
    # when the job has just gone bad.
    findings = diagnose_checkpoints(
        _overview(
            {
                "pipelineId": 1,
                "history": _history([48_000] * 8 + [4_000] * 8, interval_ms=60_000),
            }
        ),
        now_ms=NOW,
    )

    bound = _by_rule(findings, "checkpoint_bound")
    assert "48.0s" in bound.detail and "1m00s" in bound.detail


def test_checkpoints_that_finish_well_within_the_interval_are_not_flagged():
    findings = diagnose_checkpoints(
        _overview(
            {"pipelineId": 1, "history": _history([1_000] * 8, interval_ms=60_000)},
        ),
        now_ms=NOW,
    )

    assert findings == []


def test_a_stalled_checkpoint_names_the_subtasks_holding_the_barrier():
    findings = diagnose_checkpoints(
        _overview(
            {
                "pipelineId": 2,
                "inProgress": [
                    {
                        "checkpointId": 77,
                        "triggerTimestamp": NOW - (STALLED_CHECKPOINT_SECONDS + 300) * 1000,
                        "acknowledged": 3,
                        "total": 8,
                    }
                ],
            }
        ),
        now_ms=NOW,
    )

    stalled = _by_rule(findings, "checkpoint_stalled")
    assert stalled.severity == "error"
    assert "3 of 8" in stalled.detail
    assert "remaining 5" in stalled.detail
    assert "acknowledged=3/8" in stalled.evidence


def test_a_checkpoint_that_just_started_is_not_called_stalled():
    findings = diagnose_checkpoints(
        _overview(
            {
                "pipelineId": 1,
                "inProgress": [
                    {"checkpointId": 78, "triggerTimestamp": NOW - 3000, "acknowledged": 1,
                     "total": 8},
                ],
            }
        ),
        now_ms=NOW,
    )

    assert findings == []


def test_never_having_completed_a_checkpoint_is_reported():
    findings = diagnose_checkpoints(
        _overview(
            {
                "pipelineId": 1,
                "counts": {"triggered": 12, "completed": 0, "failed": 0, "inProgress": 1},
            }
        ),
        now_ms=NOW,
    )

    never = _by_rule(findings, "checkpoint_never_completed")
    assert never.evidence == "triggered=12 completed=0"


def test_a_job_in_its_first_checkpoint_interval_is_not_reported_as_never_completing():
    # triggered=1, completed=0, one in flight is what every healthy job looks
    # like before its first checkpoint lands. Saying "nothing has been
    # committed, a restart replays from the beginning" there describes normal
    # startup as a fault.
    findings = diagnose_checkpoints(
        _overview(
            {
                "pipelineId": 1,
                "counts": {"triggered": 1, "completed": 0, "failed": 0, "inProgress": 1},
                "inProgress": [{"checkpointId": 1, "triggerTimestamp": NOW - 3_000}],
            }
        ),
        now_ms=NOW,
    )

    assert findings == []


def test_a_first_checkpoint_that_is_already_overdue_is_reported():
    # One trigger is still enough once that checkpoint has been in flight
    # longer than any healthy one would be.
    findings = diagnose_checkpoints(
        _overview(
            {
                "pipelineId": 1,
                "counts": {"triggered": 1, "completed": 0, "failed": 0, "inProgress": 1},
                "inProgress": [
                    {
                        "checkpointId": 1,
                        "triggerTimestamp": NOW - (STALLED_CHECKPOINT_SECONDS + 10) * 1000,
                    }
                ],
            }
        ),
        now_ms=NOW,
    )

    never = _by_rule(findings, "checkpoint_never_completed")
    assert "oldest in progress for" in never.evidence


def test_a_second_trigger_with_nothing_completed_is_reported_without_timestamps():
    # An engine that sends counts but no inProgress detail still supports the
    # "two rounds, nothing committed" reading.
    findings = diagnose_checkpoints(
        _overview(
            {
                "pipelineId": 1,
                "counts": {
                    "triggered": MIN_TRIGGERS_NEVER_COMPLETED,
                    "completed": 0,
                    "failed": 0,
                },
            }
        ),
        now_ms=NOW,
    )

    assert "checkpoint_never_completed" in _rules(findings)


def test_a_stuck_checkpoint_is_detected_even_though_updated_at_is_stale():
    # `updatedAt` looks like a response timestamp and is not one: the engine
    # writes it inside HazelcastCheckpointOverviewStateStore.updateOverview,
    # so only when a checkpoint event is recorded. For a checkpoint hung on
    # its last subtask the newest write is that last acknowledgement, a few
    # seconds after the trigger, and nothing follows. Taking it as "now"
    # freezes the age there, and the stalled rule -- the whole point of which
    # is a hung barrier -- could never fire.
    trigger = NOW - (STALLED_CHECKPOINT_SECONDS + 30) * 1000
    overview = {
        "jobId": "852",
        "updatedAt": trigger + 3_000,
        "pipelines": [
            {
                "pipelineId": 1,
                "inProgress": [
                    {
                        "checkpointId": 7,
                        "triggerTimestamp": trigger,
                        "acknowledged": 3,
                        "total": 8,
                    }
                ],
            }
        ],
    }

    stalled = _by_rule(diagnose_checkpoints(overview, now_ms=NOW), "checkpoint_stalled")
    assert "2m30s" in stalled.detail
    assert "3 of 8 subtasks" in stalled.detail


def test_without_an_injected_clock_the_local_one_is_used_not_updated_at():
    # The fallback has to be the local clock. If `updatedAt` were consulted
    # here the age would come out as 3s and the rule would stay silent.
    now = int(time.time() * 1000)
    trigger = now - (STALLED_CHECKPOINT_SECONDS + 60) * 1000
    overview = {
        "jobId": "852",
        "updatedAt": trigger + 3_000,
        "pipelines": [
            {"pipelineId": 1, "inProgress": [{"checkpointId": 7, "triggerTimestamp": trigger}]}
        ],
    }

    assert "checkpoint_stalled" in _rules(diagnose_checkpoints(overview))


def test_failed_checkpoints_are_reported_with_the_engine_reason():
    findings = diagnose_checkpoints(
        _overview(
            {
                "pipelineId": 1,
                "counts": {"triggered": 40, "completed": 36, "failed": 4},
                "latestCompleted": {"checkpointId": 39, "triggerTimestamp": NOW - 120_000},
                "latestFailed": {
                    "checkpointId": 40,
                    "status": "FAILED",
                    "triggerTimestamp": NOW - 60_000,
                    "failureReason": "checkpoint expired before completing",
                },
            }
        ),
        now_ms=NOW,
    )

    failing = _by_rule(findings, "checkpoint_failing")
    assert "checkpoint expired before completing" in failing.detail
    assert failing.evidence == "failed=4 completed=36"


def test_old_failures_under_a_newer_completion_are_not_a_warning():
    # `counts.failed` never goes down, so one expired checkpoint keeps it above
    # zero for the rest of the job's life. A long-running pipeline that has
    # completed thousands of checkpoints since is committing progress, and
    # telling its owner it makes "no durable progress" is simply false.
    findings = diagnose_checkpoints(
        _overview(
            {
                "pipelineId": 1,
                "counts": {"triggered": 5000, "completed": 4999, "failed": 1},
                "latestFailed": {
                    "checkpointId": 12,
                    "triggerTimestamp": NOW - 20 * 86400 * 1000,
                    "failureReason": "CHECKPOINT_EXPIRED",
                },
                "latestCompleted": {"checkpointId": 5000, "triggerTimestamp": NOW - 10_000},
            }
        ),
        now_ms=NOW,
    )

    assert "checkpoint_failing" not in _rules(findings)
    note = _by_rule(findings, "checkpoint_failed_before")
    assert note.severity == "info"
    assert "committing progress now" in note.detail
    assert "no durable progress" not in note.detail


def test_a_failure_newer_than_the_last_completion_is_a_warning():
    findings = diagnose_checkpoints(
        _overview(
            {
                "pipelineId": 1,
                "counts": {"triggered": 5000, "completed": 4990, "failed": 10},
                "latestCompleted": {"checkpointId": 4990, "triggerTimestamp": NOW - 600_000},
                "latestFailed": {"checkpointId": 5000, "triggerTimestamp": NOW - 5_000},
            }
        ),
        now_ms=NOW,
    )

    failing = _by_rule(findings, "checkpoint_failing")
    assert failing.severity == "warning"
    assert "latest checkpoint failed" in failing.detail


def test_failures_with_no_completion_on_record_are_a_warning():
    # Nothing has ever completed, so the latest failure is the only outcome
    # there is.
    findings = diagnose_checkpoints(
        _overview(
            {
                "pipelineId": 1,
                "counts": {"triggered": 3, "completed": 0, "failed": 3},
                "latestFailed": {"checkpointId": 3, "triggerTimestamp": NOW - 5_000},
            }
        ),
        now_ms=NOW,
    )

    assert _by_rule(findings, "checkpoint_failing").severity == "warning"


def test_an_unknown_completed_count_is_not_reported_as_zero():
    # A count the engine did not send, or sent unusably, is unknown. Saying
    # "completed none" from it would present a guess as a measurement.
    for completed in (None, "x", {}):
        findings = diagnose_checkpoints(
            _overview(
                {"pipelineId": 1, "counts": {"triggered": 12, "completed": completed}}
            ),
            now_ms=NOW,
        )

        assert "checkpoint_never_completed" not in _rules(findings)


def test_byte_sizes_are_rendered_at_a_readable_scale():
    findings = diagnose_checkpoints(
        _overview(
            {
                "pipelineId": 1,
                "history": _history(
                    [30_000] * 8, state_sizes=[6 << 30] * 4 + [1 << 30] * 4
                ),
            }
        ),
        now_ms=NOW,
    )

    growing = _by_rule(findings, "checkpoint_state_growing")
    assert "6.0GiB" in growing.detail and "1.0GiB" in growing.detail


def test_a_cluster_with_no_checkpoint_monitor_data_yields_nothing():
    # The endpoint answers with just the jobId when the monitor service is not
    # running or has nothing for this job. That is a normal state, not an error.
    assert diagnose_checkpoints({"jobId": "852"}, now_ms=NOW) == []


def test_findings_from_several_pipelines_are_ordered_worst_first():
    findings = diagnose_checkpoints(
        {
            "jobId": "852",
            "pipelines": [
                {"pipelineId": 1, "history": _history([30_000] * 4 + [6_000] * 4)},
                {
                    "pipelineId": 2,
                    "inProgress": [
                        {
                            "checkpointId": 9,
                            "triggerTimestamp": NOW - (STALLED_CHECKPOINT_SECONDS + 10) * 1000,
                            "acknowledged": 0,
                            "total": 4,
                        }
                    ],
                },
            ],
        },
        now_ms=NOW,
    )

    # The stalled checkpoint on pipeline 2 is an error and must outrank the
    # slowdown warning on pipeline 1, even though pipeline 1 is listed first.
    assert findings[0].rule == "checkpoint_stalled"
    assert "checkpoint_duration_growing" in _rules(findings)


def test_malformed_payloads_yield_no_findings_rather_than_raising():
    for payload in (
        None,
        [],
        "nope",
        {},
        {"pipelines": None},
        {"pipelines": "none"},
        {"pipelines": [None, 7, "x"]},
        {"pipelines": [{"pipelineId": 1, "history": "none"}]},
        {"pipelines": [{"pipelineId": 1, "history": [None, 3]}]},
        {"pipelines": [{"pipelineId": 1, "history": [{"checkpoint": "no"}]}]},
        {"pipelines": [{"pipelineId": 1, "counts": "none"}]},
        {"pipelines": [{"pipelineId": 1, "counts": {"triggered": "x", "completed": None}}]},
        {"pipelines": [{"pipelineId": 1, "inProgress": "none"}]},
        {"pipelines": [{"pipelineId": 1, "inProgress": [None, 2]}]},
        {"pipelines": [{"pipelineId": 1, "inProgress": [{"triggerTimestamp": "soon"}]}]},
    ):
        assert diagnose_checkpoints(payload, now_ms=NOW) == []


def test_a_failed_count_still_reports_when_the_reason_is_unusable():
    # latestFailed can be an empty object, since the engine omits null fields,
    # and a count of failures is worth reporting with or without a reason --
    # dropping the finding because the explanation is missing would discard the
    # part that was actually measured.
    for latest_failed in ({}, "none", None, 7):
        findings = diagnose_checkpoints(
            _overview(
                {"pipelineId": 1, "counts": {"failed": 2}, "latestFailed": latest_failed}
            ),
            now_ms=NOW,
        )

        # Without a timestamp there is no evidence about the latest attempt,
        # so the count is reported as a count rather than as a live fault.
        assert _rules(findings) == ["checkpoint_failed_before"]
        assert findings[0].severity == "info"
        assert "not visible in this response" in findings[0].detail
        assert "Last failure" not in findings[0].detail


def test_identical_timestamps_do_not_divide_by_zero():
    # A clock with coarse resolution, or a burst of savepoints, can give
    # several entries the same trigger timestamp.
    findings = diagnose_checkpoints(
        _overview({"pipelineId": 1, "history": _history([9_000] * 8, interval_ms=0)}),
        now_ms=NOW,
    )

    assert "checkpoint_bound" not in _rules(findings)

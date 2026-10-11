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

"""Turn one ``/job-info`` response into findings, without calling a model.

Every rule here reads a field the engine already publishes. The ``diagnostics``
block in particular is a dedicated structure -- per-state entry timestamps at
both job and pipeline level, plus the pipeline restore counter -- so the two
questions users actually ask about a non-finishing job can be answered by
arithmetic:

* "it says RUNNING but nothing is moving" -> a pipeline is restarting in a
  loop, visible as ``restoreCount`` climbing toward ``maxRestoreCount``
* "it has been stuck for ages" -> how long ago the job entered its current
  state, from ``stateTimestamps``

Both were previously answerable only by reading server logs. The point of
doing this with rules rather than a model is that these answers are derived,
not guessed: a restore count of 7 is a crash loop whatever a model thinks.

Pure functions over dicts: no I/O, no network, no model calls. Unknown or
missing input yields fewer findings, never a wrong one -- the caller prints
what is here and says nothing about what is not.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from seatunnel_cli.diagnostics.errors import parse_error

# How long a job may sit in a non-terminal state before it is worth
# mentioning. Submitting, scheduling and deploying a pipeline legitimately
# takes a few seconds on a cold cluster, and reporting that as "stuck" would
# make the normal case look broken, so the threshold is deliberately well
# above any healthy startup.
STUCK_SECONDS = 120

# Restarting once is normal operation -- a transient connection drop costs one
# restore and the job carries on. The signal is repetition, so the crash-loop
# rule only fires from the second restore.
CRASH_LOOP_RESTORES = 2

# ``restoreCount`` counts restores since submission, so on its own it says
# nothing about *now*: a streaming job that recovered twice from transient
# errors weeks ago carries the same count as one restarting every minute. The
# distinguishing evidence is how long the pipeline has been in its current
# state -- the engine overwrites the pipeline state timestamps on each
# restore, so a recent entry into RUNNING means the latest attempt is young.
# Ten minutes is comfortably longer than the restart interval of a real loop
# (which is bounded by deploy time, seconds) and comfortably shorter than an
# uptime anyone would call recovered.
CRASH_LOOP_RECENT_SECONDS = 600

# The PipelineStatus values, grouped by what a restore count means in each.
#
# This matters because a restorable pipeline is only in an end state for an
# instant: `SubPlan.prepareRestorePipeline` increments the restore counter and
# then immediately `reset()`s the pipeline back toward CREATED, before the
# restore interval elapses. So a pipeline that a response shows *in* an end
# state is not mid-restart -- it is done, and saying "it is restarting again"
# would be false. `PipelineStatus.isEndState()` covers FINISHED, CANCELED and
# FAILED; FAILED is separated here because it is worth a past-tense note when
# the restore budget was spent, while the other two are worth nothing at all.
_PIPELINE_ENDED = {"FINISHED", "CANCELED"}
_PIPELINE_FAILED = {"FAILED"}
_PIPELINE_TEARING_DOWN = {"CANCELING", "FAILING"}
_PIPELINE_RESTARTING = {"CREATED", "SCHEDULED", "DEPLOYING", "INITIALIZING"}

# The JobStatus values declared EndState.NOT_END, split by what a long stay
# in them actually means. Terminal states are absent on purpose: there, a
# duration is just how long ago the job ended and says nothing about health.
#
# Before running, the usual cause is the cluster not granting the slots the
# job asked for. While tearing down, it is a task that will not stop. A
# savepoint is a third case: it legitimately takes as long as the state is
# large. Those need different wording, because sending someone to check
# cluster resources over a slow savepoint wastes their time.
_PRE_RUNNING = {"INITIALIZING", "CREATED", "PENDING", "SCHEDULED"}
_TEARING_DOWN = {"FAILING", "CANCELING"}
_SAVEPOINT = {"DOING_SAVEPOINT"}
_NON_TERMINAL = _PRE_RUNNING | _TEARING_DOWN | _SAVEPOINT | {"RUNNING"}

_SEVERITY_ORDER = {"error": 0, "warning": 1, "info": 2}


@dataclass(frozen=True)
class Finding:
    """One observation about a job.

    ``evidence`` is the field values the rule fired on, kept separate from
    ``detail`` so a reader can check the conclusion against the numbers rather
    than having to trust it.
    """

    severity: str
    rule: str
    detail: str
    evidence: str | None = None

    def render(self) -> str:
        line = f"[{self.severity}] {self.detail}"
        return f"{line} ({self.evidence})" if self.evidence else line


def is_non_terminal(status) -> bool:
    """Whether a job status is one the engine can still move out of.

    Exposed because the caller needs the same notion to decide whether a
    missing ``diagnostics`` block is a gap worth reporting: the engine builds
    that block only for a job the master still coordinates, so for a finished
    or cancelled job its absence is by design rather than lost coverage.
    """
    return isinstance(status, str) and status.strip().upper() in _NON_TERMINAL


def _as_int(value) -> int | None:
    """Engine JSON sends counters as ints, but metrics arrive as strings."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _state_entered_at(timestamps: dict, status: str) -> int | None:
    """Epoch millis of when the job entered ``status``, or None if unusable.

    The engine records the *last* entry into each state, keyed by state name,
    so the entry for the current status is the right answer even for a job
    that was restored and re-entered an earlier state. The map is unordered,
    so the fallback when the current status has no entry of its own is the
    largest timestamp present -- never the last key, which would be an
    arbitrary and possibly very stale state.
    """
    if not isinstance(timestamps, dict):
        return None
    own = _as_int(timestamps.get(status))
    if own and own > 0:
        return own
    values = [v for v in (_as_int(v) for v in timestamps.values()) if v and v > 0]
    return max(values) if values else None


def _humanize(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m{seconds % 60:02d}s"
    return f"{seconds // 3600}h{(seconds % 3600) // 60:02d}m"


def _failure_findings(job_info: dict) -> list[Finding]:
    error_msg = job_info.get("errorMsg")
    if not isinstance(error_msg, str) or not error_msg.strip():
        return [
            Finding(
                "error",
                "failed_without_error",
                "The job failed but reported no error message; the cause is only in the server log.",
            )
        ]
    parsed = parse_error(error_msg)
    findings = [Finding("error", "job_failed", parsed.headline())]
    if parsed.hint:
        findings.append(Finding("info", "failure_hint", parsed.hint))
    return findings


def _crash_loop_findings(pipelines: list, now_ms: int) -> list[Finding]:
    """Restore counters, reported as a live problem only when they are one.

    ``restoreCount`` is cumulative, so what it means depends entirely on the
    pipeline's current state. The grouping above encodes the engine's own
    lifecycle: a restorable pipeline occupies an end state for an instant
    only, because ``SubPlan.prepareRestorePipeline`` increments the counter
    and immediately ``reset()``s the pipeline back toward ``CREATED`` before
    the restore interval elapses. A pipeline observed in an end state is
    therefore finished, not restarting, and its restore count is history.
    """
    findings = []
    for pipeline in pipelines:
        if not isinstance(pipeline, dict):
            continue
        restores = _as_int(pipeline.get("restoreCount"))
        if restores is None or restores < CRASH_LOOP_RESTORES:
            continue
        pipeline_id = pipeline.get("pipelineId", "?")
        limit = _as_int(pipeline.get("maxRestoreCount"))
        exhausted = limit is not None and limit > 0 and restores >= limit

        status = pipeline.get("pipelineStatus")
        status = status.strip().upper() if isinstance(status, str) and status.strip() else None

        evidence = f"restoreCount={restores}"
        if limit is not None and limit > 0:
            evidence += f"/{limit}"
        if status is not None:
            evidence += f" pipelineStatus={status}"

        # Finished or cancelled: the pipeline is done. In a multi-pipeline job
        # one pipeline finishing after two earlier restores is normal, and
        # calling it a crash loop would be plainly wrong.
        if status in _PIPELINE_ENDED:
            continue

        # Failed: past tense. A failure that was not restorable, or one after
        # the budget ran out, is covered by the job-level failure finding; the
        # only thing worth adding is that the restores were all spent.
        if status in _PIPELINE_FAILED:
            if exhausted:
                findings.append(
                    Finding(
                        "error",
                        "pipeline_restore_exhausted",
                        f"Pipeline {pipeline_id} used all {restores} of its restores and "
                        f"then failed.",
                        evidence,
                    )
                )
            continue

        # Budget gone while the pipeline is still live: the engine will not
        # restore again, so the next failure is terminal.
        if exhausted:
            findings.append(
                Finding(
                    "error",
                    "pipeline_restore_exhausted",
                    f"Pipeline {pipeline_id} has restarted {restores} times, which is "
                    f"its limit: the next failure ends the job.",
                    evidence,
                )
            )
            continue

        # Shutting down. Whatever the restores were, they are not what is
        # happening now, and the stop may well have been requested.
        if status in _PIPELINE_TEARING_DOWN:
            findings.append(
                Finding(
                    "info",
                    "pipeline_restored_before",
                    f"Pipeline {pipeline_id} restarted {restores} times since it was "
                    f"submitted and is now {status}.",
                    evidence,
                )
            )
            continue

        # Between attempts: not yet running, with restores behind it, which is
        # what a restart in progress looks like.
        if status in _PIPELINE_RESTARTING:
            findings.append(
                Finding(
                    "warning",
                    "pipeline_crash_loop",
                    f"Pipeline {pipeline_id} has restarted {restores} times and is "
                    f"{status} rather than RUNNING, so it is between attempts rather "
                    f"than processing data.",
                    evidence,
                )
            )
            continue

        if status == "RUNNING":
            attempt_started = _state_entered_at(pipeline.get("stateTimestamps", {}), "RUNNING")
            if attempt_started is not None:
                uptime_seconds = (now_ms - attempt_started) / 1000.0
                evidence += f" running for {_humanize(uptime_seconds)} since the last restore"
                if uptime_seconds >= CRASH_LOOP_RECENT_SECONDS:
                    # Running steadily since the last restore: the count is a
                    # record of past incidents, not a description of now.
                    findings.append(
                        Finding(
                            "info",
                            "pipeline_restored_before",
                            f"Pipeline {pipeline_id} has restarted {restores} times since "
                            f"it was submitted, most recently "
                            f"{_humanize(uptime_seconds)} ago, and has been running since.",
                            evidence,
                        )
                    )
                    continue
                # Restarted recently and running again. That the restores are
                # still ongoing is not something one snapshot can show, so
                # this reports the two facts and points at the logs that can.
                findings.append(
                    Finding(
                        "warning",
                        "pipeline_crash_loop",
                        f"Pipeline {pipeline_id} has restarted {restores} times, and the "
                        f"current attempt is only {_humanize(uptime_seconds)} old. Check "
                        f"the task logs for what triggered the restores.",
                        evidence,
                    )
                )
                continue

        # No usable status, or RUNNING without a timestamp: the count is all
        # there is, so report exactly that and let the reader judge. An
        # unproven "crash loop" in the output is worse than a plain number.
        findings.append(
            Finding(
                "info",
                "pipeline_restored_before",
                f"Pipeline {pipeline_id} has restarted {restores} times since it was "
                f"submitted. How recently is not visible in this response.",
                evidence,
            )
        )
    return findings


def _stuck_findings(job_info: dict, diagnostics: dict, now_ms: int) -> list[Finding]:
    status = job_info.get("jobStatus")
    if not isinstance(status, str):
        return []
    status = status.upper()
    if status not in _NON_TERMINAL:
        return []

    entered = _state_entered_at(diagnostics.get("stateTimestamps", {}), status)
    if entered is None:
        return []
    held_seconds = (now_ms - entered) / 1000.0
    if held_seconds < STUCK_SECONDS:
        return []

    held = _humanize(held_seconds)
    if status == "RUNNING":
        # RUNNING for a long time is what a streaming job is supposed to do,
        # so this is only worth a line of context, never a warning.
        return [Finding("info", "running_duration", f"Running for {held}.", f"state={status}")]
    if status in _PRE_RUNNING:
        return [
            Finding(
                "warning",
                "stuck_before_running",
                f"The job has been {status} for {held} without starting. That usually "
                f"means the cluster could not give it the slots it asked for -- check "
                f"free slots and worker count on the cluster.",
                f"state={status}",
            )
        ]
    if status in _SAVEPOINT:
        # A savepoint writes out the whole job state, so a large state takes
        # minutes by design. Calling that "not responding" sends someone
        # looking for a hung task when the job is doing exactly what it was
        # asked to do, so this only reports the duration.
        return [
            Finding(
                "warning",
                "savepoint_in_progress",
                f"The job has been {status} for {held}. A savepoint takes as long as "
                f"the job state is large, so this can be normal; if it does not finish, "
                f"a task is not completing its snapshot.",
                f"state={status}",
            )
        ]
    return [
        Finding(
            "warning",
            "stuck_tearing_down",
            f"The job has been {status} for {held}. It is shutting down and not "
            f"finishing, which means a task is not responding to the stop request.",
            f"state={status}",
        )
    ]


def _is_streaming(job_info: dict) -> bool:
    """Whether the job declares ``job.mode = STREAMING`` in its env options.

    Only used to pick wording. A streaming source that has not produced a row
    yet may simply have nothing to produce -- a quiet topic, a CDC job waiting
    for its first change -- while a batch source that reads nothing has a
    problem by definition.
    """
    env = job_info.get("envOptions")
    mode = env.get("job.mode") if isinstance(env, dict) else None
    return isinstance(mode, str) and mode.strip().upper() == "STREAMING"


def _progress_findings(job_info: dict, diagnostics: dict, now_ms: int) -> list[Finding]:
    """Row counts, read from the job-wide scalar metrics.

    ``SourceReceivedCount``/``SinkWriteCount`` are the cluster-wide totals.
    The ``Table``-prefixed names are deliberately *not* used here: those are
    objects keyed by table name, not scalars, and every metric leaf is
    rendered as a string by the engine, so both the nesting and the quoting
    have to be respected rather than assumed.

    Both rules are gated on how long the job has been RUNNING. A zero counter
    a second after the job started running is the expected reading, not a
    symptom, and reporting it would make every freshly submitted job look
    broken.
    """
    if job_info.get("jobStatus") != "RUNNING":
        return []
    metrics = job_info.get("metrics")
    if not isinstance(metrics, dict):
        return []
    read = _as_int(metrics.get("SourceReceivedCount"))
    written = _as_int(metrics.get("SinkWriteCount"))
    if read is None or written is None:
        return []

    # The RUNNING entry specifically, not the newest timestamp of any state:
    # without it there is no way to tell a job that has read nothing for an
    # hour from one that started two seconds ago, and guessing is what these
    # rules are supposed to avoid.
    timestamps = diagnostics.get("stateTimestamps")
    entered = _as_int(timestamps.get("RUNNING")) if isinstance(timestamps, dict) else None
    if entered is None or entered <= 0:
        return []
    running_seconds = (now_ms - entered) / 1000.0
    if running_seconds < STUCK_SECONDS:
        return []
    running_for = _humanize(running_seconds)

    if read == 0:
        if _is_streaming(job_info):
            return [
                Finding(
                    "info",
                    "no_rows_read",
                    f"The source has produced no rows in the {running_for} since the job "
                    f"started running. For a streaming source with nothing to read yet -- "
                    f"a quiet topic, a CDC job waiting for its first change -- that is "
                    f"normal; if you expected rows by now, check the table or path it "
                    f"points at and any filter on the source.",
                    f"SourceReceivedCount=0 running for {running_for}",
                )
            ]
        return [
            Finding(
                "warning",
                "no_rows_read",
                f"The source has produced no rows in the {running_for} since the job "
                f"started running. Check the table or path it points at, and any filter "
                f"or where clause on the source.",
                f"SourceReceivedCount=0 running for {running_for}",
            )
        ]
    if written == 0:
        return [
            Finding(
                "warning",
                "rows_read_none_written",
                f"The source read {read} rows but the sink wrote none, so rows are "
                "being dropped between them -- usually a transform filter, or a "
                "sink that has not committed its first batch yet.",
                f"SourceReceivedCount={read} SinkWriteCount=0 running for {running_for}",
            )
        ]
    return []


def diagnose_job(job_info: dict, *, now_ms: int | None = None) -> list[Finding]:
    """Apply every rule to one ``/job-info`` response, worst finding first.

    ``now_ms`` is injectable because the stuck-state rule is a clock
    comparison, and a test that cannot pin the clock can only assert on
    thresholds it has to reach by sleeping.

    When it is not injected, ``diagnostics.generatedAt`` is preferred over the
    local clock: every timestamp it is compared against was written by the
    master, so using the master's own "now" keeps the arithmetic inside one
    clock. A laptop a few minutes off a cluster would otherwise invent a
    stuck job, or hide one.
    """
    if not isinstance(job_info, dict):
        return []

    diagnostics = job_info.get("diagnostics")
    diagnostics = diagnostics if isinstance(diagnostics, dict) else {}
    pipelines = diagnostics.get("pipelines")
    pipelines = pipelines if isinstance(pipelines, list) else []

    if now_ms is None:
        generated_at = _as_int(diagnostics.get("generatedAt"))
        now_ms = (
            generated_at
            if generated_at is not None and generated_at > 0
            else int(time.time() * 1000)
        )

    findings: list[Finding] = []
    if job_info.get("jobStatus") == "FAILED":
        findings += _failure_findings(job_info)
    findings += _crash_loop_findings(pipelines, now_ms)
    findings += _stuck_findings(job_info, diagnostics, now_ms)
    findings += _progress_findings(job_info, diagnostics, now_ms)

    # Stable sort, so rules of equal severity keep the order above: failure
    # first, then why it is not progressing, then throughput.
    findings.sort(key=lambda f: _SEVERITY_ORDER.get(f.severity, 9))
    return findings

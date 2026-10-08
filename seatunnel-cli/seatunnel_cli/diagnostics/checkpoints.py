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

"""Turn one ``/jobs/checkpoints/{jobId}`` response into findings.

The rules in ``jobs.py`` all compare against zero or against a fixed
threshold, which answers "did it break" and "is it stuck" but not the question
people actually ask about a long-running CDC job: it was fine at first and now
it lags. The counters keep moving there, just more slowly, so nothing those
rules look at ever trips.

This endpoint closes that gap because it is the one place the engine publishes
a *series* rather than a current value: up to 32 history entries per pipeline,
each with how long that checkpoint took and how big its state was. Comparing a
job against its own recent past then becomes arithmetic.

Two independent signals are worth separating, because they fail to answer
different questions:

* a **trend** -- recent checkpoints are slower than older ones. Needs a
  baseline, so it only sees degradation still visible inside the retained
  window (32 checkpoints; at a 10s interval that is roughly five minutes, not
  three days).
* a **ratio** -- checkpoints take about as long as the gap between them, so the
  job is checkpoint-bound right now. Needs no baseline at all, which is what
  makes it useful for the job that got slower yesterday and then stayed slow.

Pure functions over dicts: no I/O, no network, no model calls. Unknown or
missing input yields fewer findings, never a wrong one.
"""

from __future__ import annotations

import time

from seatunnel_cli.diagnostics.jobs import Finding, as_int, sort_findings

# A checkpoint in progress for longer than this is worth mentioning. The
# endpoint does not publish the configured interval, so there is no way to
# scale this to the job; the value is deliberately well above any healthy
# checkpoint so that a slow-but-working job is not reported as stalled.
# Note how this sits against the engine's own timeout: the packaged
# config/seatunnel.yaml sets `checkpoint.timeout: 60000`, so on default
# settings a hung checkpoint expires after 60s and surfaces through
# `checkpoint_failing` instead. This rule therefore earns its keep on
# clusters that have raised `checkpoint.timeout` above 120s, where a barrier
# can hang for minutes without the engine calling it failed.
STALLED_CHECKPOINT_SECONDS = 120

# "Triggered but never completed" is the normal reading for the first interval
# of every healthy job, so it takes a second round before it means anything.
MIN_TRIGGERS_NEVER_COMPLETED = 2

# Halving the history to compare medians needs enough samples that one slow
# outlier cannot carry the result. Six is the smallest split where each side
# has a real median rather than a single value or a pair.
MIN_TREND_SAMPLES = 6

# Doubling is a large enough step that it is not measurement noise, and it is
# the point where a user would describe the job as having got slower.
GROWTH_FACTOR = 2.0

# Below this, a doubling is arithmetically true and operationally irrelevant:
# 8ms to 20ms is not why a CDC pipeline is lagging, and reporting it would
# train people to ignore the rule.
DURATION_FLOOR_MS = 5_000

# Same reasoning for state: a few KiB doubling says nothing.
STATE_FLOOR_BYTES = 1 << 20

# At this fraction of the trigger interval the job is spending most of its
# time checkpointing, so the next checkpoint starts as the previous one ends
# and there is little room left for actual throughput.
INTERVAL_SATURATION = 0.8

_COMPLETED = "COMPLETED"


def _ms(millis: float) -> str:
    """Format a millisecond duration the way an operator would say it."""
    millis = int(millis)
    if millis < 1000:
        return f"{millis}ms"
    seconds = millis / 1000.0
    if seconds < 60:
        return f"{seconds:.1f}s"
    seconds = int(seconds)
    if seconds < 3600:
        return f"{seconds // 60}m{seconds % 60:02d}s"
    return f"{seconds // 3600}h{(seconds % 3600) // 60:02d}m"


def _bytes(count: float) -> str:
    count = float(count)
    if count < 1024:
        return f"{count:.0f}B"
    for unit in ("KiB", "MiB"):
        count /= 1024
        if count < 1024:
            return f"{count:.1f}{unit}"
    return f"{count / 1024:.1f}GiB"


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[middle])
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def _checkpoints(pipeline: dict) -> list[dict]:
    """The ``checkpoint`` objects of one pipeline's history, newest first.

    Order matters and is not obvious: the engine prepends each new entry
    (``PipelineCheckpointOverview.addHistory`` uses ``addFirst``) and the
    history endpoint sorts by ``triggerTimestamp`` descending. A trend read in
    array order without accounting for that reports the opposite conclusion,
    so it is stated once here and relied on by every caller below. The test
    suite pins it from the other direction too, with a history that is getting
    faster rather than slower.
    """
    history = pipeline.get("history")
    if not isinstance(history, list):
        return []
    out = []
    for entry in history:
        if not isinstance(entry, dict):
            continue
        checkpoint = entry.get("checkpoint")
        if isinstance(checkpoint, dict):
            out.append(checkpoint)
    return out


def _completed_only(checkpoints: list[dict]) -> list[dict]:
    """Drop everything that is not a completed checkpoint.

    Failed and canceled checkpoints are in the same history array, and both
    carry misleading values rather than absent ones: ``durationMillis`` and
    ``completedTimestamp`` are omitted from the JSON when null, while
    ``stateSize`` is a primitive ``long`` and so arrives as a truthful-looking
    ``0``. Left in, they would drag a duration trend toward nothing and make a
    growing state look like a shrinking one.
    """
    return [c for c in checkpoints if c.get("status") == _COMPLETED]


def _trend(values: list[float]) -> tuple[float, float] | None:
    """Medians of (recent half, older half), or None without enough samples.

    ``values`` is newest-first, so the recent half is the front of the list.
    """
    if len(values) < MIN_TREND_SAMPLES:
        return None
    half = len(values) // 2
    return _median(values[:half]), _median(values[half:])


def _recent_median(values: list[float]) -> float:
    """Median of the recent half, or of everything when samples are few.

    Taking the median across the whole window would answer "what was this job
    like on average recently", which is not the question. A job that was
    healthy for most of the retained history and is checkpoint-bound now gets
    its current state diluted by its own healthy past, and the rule stays
    silent exactly when it is needed.
    """
    if len(values) >= MIN_TREND_SAMPLES:
        return _median(values[: len(values) // 2])
    return _median(values)


def _duration_findings(pipeline_id, completed: list[dict]) -> list[Finding]:
    durations = [d for d in (as_int(c.get("durationMillis")) for c in completed) if d is not None]
    trend = _trend([float(d) for d in durations])
    if trend is None:
        return []
    recent, older = trend
    if recent < DURATION_FLOOR_MS or older <= 0 or recent < older * GROWTH_FACTOR:
        return []
    return [
        Finding(
            "warning",
            "checkpoint_duration_growing",
            f"Pipeline {pipeline_id} checkpoints are getting slower: recently "
            f"{_ms(recent)} against {_ms(older)} earlier in the retained history. "
            f"A checkpoint that takes longer every round holds up the source, so "
            f"throughput falls even though the job stays RUNNING.",
            f"median durationMillis {int(older)} -> {int(recent)} "
            f"over {len(durations)} checkpoints",
        )
    ]


def _state_findings(pipeline_id, completed: list[dict]) -> list[Finding]:
    sizes = [s for s in (as_int(c.get("stateSize")) for c in completed) if s is not None]
    trend = _trend([float(s) for s in sizes])
    if trend is None:
        return []
    recent, older = trend
    if recent < STATE_FLOOR_BYTES or older <= 0 or recent < older * GROWTH_FACTOR:
        return []
    return [
        Finding(
            "info",
            "checkpoint_state_growing",
            f"Pipeline {pipeline_id} checkpoint state is growing: recently "
            f"{_bytes(recent)} against {_bytes(older)} earlier. This is usually what "
            f"is behind checkpoints taking longer.",
            f"median stateSize {int(older)} -> {int(recent)} bytes",
        )
    ]


def _interval_findings(
    pipeline_id, checkpoints: list[dict], completed: list[dict]
) -> list[Finding]:
    """Compare how long checkpoints take against how often they are triggered.

    Unlike the trend rules this needs no baseline, so it still fires on a job
    that degraded before the retained window began and has been slow ever
    since -- which is the common case by the time someone asks for help.

    Intervals come from every history entry, not just the completed ones: a
    checkpoint that failed was still triggered on schedule, and dropping it
    would overstate the gap between triggers.
    """
    triggers = [t for t in (as_int(c.get("triggerTimestamp")) for c in checkpoints) if t]
    durations = [d for d in (as_int(c.get("durationMillis")) for c in completed) if d is not None]
    if len(triggers) < 3 or len(durations) < 3:
        return []
    # Newest first, so the earlier element is the later timestamp.
    gaps = [
        float(triggers[i] - triggers[i + 1])
        for i in range(len(triggers) - 1)
        if triggers[i] > triggers[i + 1]
    ]
    if len(gaps) < 2:
        return []
    interval = _recent_median(gaps)
    duration = _recent_median([float(d) for d in durations])
    if interval <= 0 or duration < interval * INTERVAL_SATURATION:
        return []
    return [
        Finding(
            "warning",
            "checkpoint_bound",
            f"Pipeline {pipeline_id} now spends almost all its time checkpointing: "
            f"a checkpoint takes {_ms(duration)} and one is triggered every "
            f"{_ms(interval)}. Raise checkpoint.interval, or find why the snapshot "
            f"is slow -- until then the job cannot go faster than its checkpoints.",
            f"recent median duration {int(duration)}ms vs interval {int(interval)}ms",
        )
    ]


def _triggered_at(pipeline: dict, key: str) -> int | None:
    """``triggerTimestamp`` of ``latestCompleted`` / ``latestFailed``, if usable."""
    latest = pipeline.get(key)
    if not isinstance(latest, dict):
        return None
    stamp = as_int(latest.get("triggerTimestamp"))
    return stamp if stamp and stamp > 0 else None


def _oldest_in_progress_age_ms(pipeline: dict, now_ms: int) -> int | None:
    in_progress = pipeline.get("inProgress")
    if not isinstance(in_progress, list):
        return None
    ages = [
        now_ms - stamp
        for stamp in (
            as_int(c.get("triggerTimestamp")) for c in in_progress if isinstance(c, dict)
        )
        if stamp and stamp > 0
    ]
    return max(ages) if ages else None


def _counts_findings(pipeline_id, pipeline: dict, now_ms: int) -> list[Finding]:
    """Rules over ``counts``, which are lifetime totals rather than a state.

    Every value here counts since the job was submitted, so neither "failed"
    nor "completed none" says anything about the present on its own. Both
    rules therefore need a second piece of evidence -- the relative age of the
    latest failure and completion, or how many rounds have been triggered --
    before they are allowed to describe the job as it is now.
    """
    counts = pipeline.get("counts")
    if not isinstance(counts, dict):
        return []
    triggered = as_int(counts.get("triggered"))
    completed = as_int(counts.get("completed"))
    failed = as_int(counts.get("failed"))

    findings = []
    # `completed == 0`, not `not completed`: an absent or unparsable count is
    # unknown, and reporting "completed none" from it would state as measured
    # something the engine never said.
    if triggered and completed == 0:
        # A job that has just started legitimately reads triggered=1,
        # completed=0 with that first checkpoint still in flight. Saying
        # "nothing has been committed" there describes every healthy job in
        # its first interval, so it needs either a second round or a first
        # round that has already been running too long.
        stalled_ms = _oldest_in_progress_age_ms(pipeline, now_ms)
        rounds_enough = triggered >= MIN_TRIGGERS_NEVER_COMPLETED
        first_round_overdue = stalled_ms is not None and (
            stalled_ms >= STALLED_CHECKPOINT_SECONDS * 1000
        )
        if rounds_enough or first_round_overdue:
            evidence = f"triggered={triggered} completed=0"
            if stalled_ms is not None:
                evidence += f" oldest in progress for {_ms(stalled_ms)}"
            findings.append(
                Finding(
                    "warning",
                    "checkpoint_never_completed",
                    f"Pipeline {pipeline_id} has triggered {triggered} checkpoints and "
                    f"completed none. Nothing has been committed, so a restart replays "
                    f"from the beginning rather than resuming.",
                    evidence,
                )
            )
    if failed:
        reason = pipeline.get("latestFailed")
        reason = reason.get("failureReason") if isinstance(reason, dict) else None
        failed_at = _triggered_at(pipeline, "latestFailed")
        completed_at = _triggered_at(pipeline, "latestCompleted")
        evidence = f"failed={failed}"
        if completed is not None:
            evidence += f" completed={completed}"

        # The deciding question is not "has anything ever failed" but "did the
        # last attempt fail". A pipeline with one expired checkpoint weeks ago
        # and thousands of completions since keeps `failed` above zero for the
        # rest of its life, and a warning claiming it makes "no durable
        # progress" would be wrong for as long as it runs.
        failing_now = failed_at is not None and (completed_at is None or failed_at > completed_at)
        if failing_now:
            detail = (
                f"Pipeline {pipeline_id} has {failed} failed checkpoints and its latest "
                f"checkpoint failed rather than completed. Offsets stop advancing on "
                f"every failure, so the job can appear to be reading while making no "
                f"durable progress."
            )
            if isinstance(reason, str) and reason.strip():
                detail += f" Last failure: {reason.strip()}"
            findings.append(Finding("warning", "checkpoint_failing", detail, evidence))
        elif completed_at is not None:
            detail = (
                f"Pipeline {pipeline_id} has {failed} failed checkpoints since it was "
                f"submitted, but the most recent checkpoint completed, so it is "
                f"committing progress now."
            )
            if isinstance(reason, str) and reason.strip():
                detail += f" Last failure was: {reason.strip()}"
            findings.append(Finding("info", "checkpoint_failed_before", detail, evidence))
        else:
            # Counter present, no usable timestamps: report the count and the
            # reason, but not a conclusion about now.
            detail = (
                f"Pipeline {pipeline_id} has {failed} failed checkpoints since it was "
                f"submitted. Whether the latest one failed is not visible in this "
                f"response."
            )
            if isinstance(reason, str) and reason.strip():
                detail += f" Last failure was: {reason.strip()}"
            findings.append(Finding("info", "checkpoint_failed_before", detail, evidence))
    return findings


def _in_progress_findings(pipeline_id, pipeline: dict, now_ms: int) -> list[Finding]:
    in_progress = pipeline.get("inProgress")
    if not isinstance(in_progress, list):
        return []
    findings = []
    for checkpoint in in_progress:
        if not isinstance(checkpoint, dict):
            continue
        triggered_at = as_int(checkpoint.get("triggerTimestamp"))
        if not triggered_at or triggered_at <= 0:
            continue
        age_ms = now_ms - triggered_at
        if age_ms < STALLED_CHECKPOINT_SECONDS * 1000:
            continue
        acknowledged = as_int(checkpoint.get("acknowledged"))
        total = as_int(checkpoint.get("total"))
        checkpoint_id = checkpoint.get("checkpointId", "?")
        detail = (
            f"Pipeline {pipeline_id} checkpoint {checkpoint_id} has been in progress "
            f"for {_ms(age_ms)} without completing."
        )
        evidence = f"triggerTimestamp={triggered_at}"
        # Naming the subtasks that have not acknowledged turns "the job is slow"
        # into "this stage is slow", which is the difference between a usable
        # diagnosis and a restatement of the symptom.
        if acknowledged is not None and total:
            detail += (
                f" {acknowledged} of {total} subtasks have acknowledged it, so the "
                f"remaining {total - acknowledged} are what the barrier is waiting on."
            )
            evidence += f" acknowledged={acknowledged}/{total}"
        findings.append(Finding("error", "checkpoint_stalled", detail, evidence))
    return findings


def diagnose_checkpoints(overview: dict, *, now_ms: int | None = None) -> list[Finding]:
    """Apply every checkpoint rule to one overview response, worst finding first.

    The response carries only ``jobId`` when the monitor service is not
    running or has nothing recorded for the job, which is a legitimate state
    rather than an error: it yields no findings.

    ``now_ms`` should be the engine's own "now" where the caller has one --
    ``diagnostics.generatedAt`` from the ``/job-info`` response fetched moments
    earlier, which the master stamps at response time. The local clock is the
    fallback.

    Deliberately *not* the overview's own ``updatedAt``, which looks like the
    right field and is not: the engine writes it inside
    ``HazelcastCheckpointOverviewStateStore.updateOverview``, i.e. only when a
    checkpoint event is recorded. For a checkpoint stuck waiting on its last
    subtask the final write is that last acknowledgement, and nothing is
    written afterwards, so ``updatedAt - triggerTimestamp`` freezes a few
    seconds after the trigger and never reaches the stalled threshold. Using
    it would make ``checkpoint_stalled`` unable to fire in the one case it
    exists for.
    """
    if not isinstance(overview, dict):
        return []

    if now_ms is None:
        now_ms = int(time.time() * 1000)

    pipelines = overview.get("pipelines")
    if not isinstance(pipelines, list):
        return []

    findings: list[Finding] = []
    for pipeline in pipelines:
        if not isinstance(pipeline, dict):
            continue
        pipeline_id = pipeline.get("pipelineId", "?")
        checkpoints = _checkpoints(pipeline)
        completed = _completed_only(checkpoints)

        findings += _in_progress_findings(pipeline_id, pipeline, now_ms)
        findings += _counts_findings(pipeline_id, pipeline, now_ms)
        findings += _duration_findings(pipeline_id, completed)
        findings += _interval_findings(pipeline_id, checkpoints, completed)
        findings += _state_findings(pipeline_id, completed)
    return sort_findings(findings)

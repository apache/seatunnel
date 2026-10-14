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

"""Correlate retained queue pressure with downstream work, without model I/O.

These are candidates to investigate, not connector/root-cause attribution.
Only the latest three distinct, consecutive, completed and matched buckets
count; one spike, a partial bucket or three reads of one bucket do not.
"""

from __future__ import annotations

import math
from statistics import median

from .jobs import Finding

MIN_BUCKETS = 3
BACKPRESSURE_RATIO = 0.5
QUEUE_FILL_RATIO = 0.8
BUSY_RATIO = 0.5


def _integer(value):
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value if math.isfinite(value) and value >= 0 else None


def _ratio(value):
    value = _number(value)
    return value if value is not None and value <= 1 else None


def _window(data, key):
    if not isinstance(data, dict) or not isinstance(data.get(key), list):
        return None
    values = tuple(_integer(data.get(k)) for k in ("fromMs", "toMs", "bucketMs"))
    start, end, width = values
    if any(v is None for v in values) or start < 0 or end <= start or width <= 0:
        return None
    return values


def _points(series, start, end, width):
    """Index complete aligned buckets, discarding conflicting duplicate data."""
    indexed = {}
    conflicts = set()
    points = series.get("points")
    if not isinstance(points, list):
        return indexed
    for point in points:
        if not isinstance(point, dict):
            continue
        ts = _integer(point.get("ts"))
        if ts is None or ts < start or ts + width > end or ts % width:
            continue
        if ts in indexed and indexed[ts] != point:
            conflicts.add(ts)
        indexed[ts] = point
    return {ts: point for ts, point in indexed.items() if ts not in conflicts}


def _recent_run(timestamps, width, end):
    """Require the window's latest complete buckets, not a stale matching run."""
    complete_end = (end // width) * width
    required = [complete_end - i * width for i in range(MIN_BUCKETS, 0, -1)]
    return required if all(ts in timestamps for ts in required) else []


def _unavailable(reason):
    return Finding(
        "info",
        "realtime_unavailable",
        f"Realtime pressure checks unavailable: {reason}.",
    )


def diagnose_realtime(edges, vertices) -> list[Finding]:
    """Read the engine's edges/vertices responses, using their shared clock window."""
    if any(
        isinstance(data, dict) and data.get("enabled") is False
        for data in (edges, vertices)
    ):
        return [_unavailable("engine.observability.enabled is false")]
    edge_window, vertex_window = _window(edges, "edges"), _window(vertices, "vertices")
    if edge_window is None or vertex_window is None:
        return [_unavailable("an endpoint or its metric window is missing")]
    if edge_window[2] != vertex_window[2]:
        return [_unavailable("the responses have different bucket widths")]
    start, end = max(edge_window[0], vertex_window[0]), min(
        edge_window[1], vertex_window[1]
    )
    width = edge_window[2]
    if end - start < MIN_BUCKETS * width:
        return [_unavailable("fewer than three complete buckets fit the shared window")]

    vertex_points = {}
    for series in vertices["vertices"]:
        if isinstance(series, dict):
            vertex_id = _integer(series.get("vertexId"))
            if vertex_id is not None:
                vertex_points[vertex_id] = _points(series, start, end, width)

    findings = []
    matched_runs = 0
    incomplete_queues = 0
    for edge in edges["edges"]:
        if not isinstance(edge, dict):
            incomplete_queues += 1
            continue
        queue_id, target = _integer(edge.get("queueId")), _integer(
            edge.get("targetVertexId")
        )
        if queue_id is None or target is None:
            incomplete_queues += 1
            continue
        ep = _points(edge, start, end, width)
        vp = vertex_points.get(target, {})
        run = _recent_run(ep.keys() & vp.keys(), width, end)
        if not run:
            incomplete_queues += 1
            continue
        # The engine clamps vertex/fill ratios, but publishes bpRatio without
        # an upper clamp. Preserve values above one from delayed collection.
        bp = [_number(ep[ts].get("bpRatio")) for ts in run]
        fill = [_ratio(ep[ts].get("queueFillRatio")) for ts in run]
        if any(v is None for v in bp + fill):
            incomplete_queues += 1
            continue
        matched_runs += 1
        if not all(
            b >= BACKPRESSURE_RATIO and f >= QUEUE_FILL_RATIO for b, f in zip(bp, fill)
        ):
            continue
        detail = f"Queue {queue_id} into vertex {target} was persistently backpressured in the matched window."
        for key, label in (
            ("sinkBusyRatio", "sink writes"),
            ("transformBusyRatio", "transform processing"),
        ):
            busy = [_ratio(vp[ts].get(key)) for ts in run]
            if all(v is not None and v >= BUSY_RATIO for v in busy):
                detail += f" Downstream {label} were busy too; investigate this vertex as a bottleneck candidate."
                break
        else:
            detail += " The downstream work metrics do not establish the cause; low write busyness does not exclude commit pressure."
        findings.append(
            Finding(
                "warning",
                "realtime_queue_pressure",
                detail,
                f"queueId={queue_id} targetVertexId={target} buckets={len(run)} "
                f"window={run[0]}..{run[-1] + width}ms "
                f"median bpRatio={median(bp):.2f} queueFillRatio={median(fill):.2f}",
            )
        )

    for vertex_id, points in vertex_points.items():
        run = _recent_run(points, width, end)
        if not run:
            continue
        ratios = []
        for ts in run:
            point = points[ts]
            prepare, commit = _number(point.get("sinkPrepareCommitNs")), _number(
                point.get("sinkCommitNs")
            )
            tasks = _integer(point.get("subtaskCount"))
            if prepare is None or commit is None or tasks is None or tasks <= 0:
                break
            ratios.append((prepare + commit) / (width * 1_000_000 * tasks))
        if len(ratios) == len(run) and all(value >= BUSY_RATIO for value in ratios):
            findings.append(
                Finding(
                    "warning",
                    "realtime_sink_commit_pressure",
                    f"Vertex {vertex_id} spent substantial time preparing/committing across three completed buckets. "
                    "Investigate checkpoint/commit work; sinkBusyRatio measures writes only.",
                    f"vertexId={vertex_id} buckets={len(run)} window={run[0]}..{run[-1] + width}ms "
                    f"median prepare+commit ratio={median(ratios):.2f}",
                )
            )
    if not matched_runs:
        findings.append(
            _unavailable(
                "no three consecutive queue/target buckets were available for attribution"
            )
        )
    elif incomplete_queues:
        findings.append(
            _unavailable(
                f"{incomplete_queues} queue series lacked matched, usable buckets"
            )
        )
    return findings

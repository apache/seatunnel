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

"""Scoring for benchmark task results.

Correctness checks per task (RFC Section 5.3, Table 4):
  parse_success      — generated HOCON parses (pyhocon) and passes local
                       structural validation (validate_hocon)
  field_completeness — no "missing required options" errors against
                       connector metadata
  connector_match    — expected source/sink/transform connectors present
  job_mode_match     — env job.mode matches the expected mode
  content_match      — task-specific must_match regexes all hit and
                       must_not_match regexes all miss
  engine_check       — optional: `seatunnel.sh --check` passes (needs
                       SEATUNNEL_HOME; skipped otherwise)

Composite task score (0..1) weights correctness dimensions; a task counts
as "passed" when all non-optional checks succeed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from seatunnel_cli.agents import validate_hocon, dry_run_config


# Weights for the composite score. Engine check is scored only when it ran.
WEIGHTS = {
    "parse_success": 0.25,
    "field_completeness": 0.20,
    "connector_match": 0.25,
    "job_mode_match": 0.10,
    "content_match": 0.20,
}


@dataclass
class TaskScore:
    task_id: str
    checks: dict = field(default_factory=dict)     # name -> bool
    details: dict = field(default_factory=dict)    # name -> str notes
    engine_check: str | None = None                # PASS / FAIL:... / None=skipped

    @property
    def score(self) -> float:
        total = 0.0
        for name, weight in WEIGHTS.items():
            if self.checks.get(name):
                total += weight
        return round(total, 4)

    @property
    def passed(self) -> bool:
        return all(self.checks.get(name, False) for name in WEIGHTS)

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "passed": self.passed,
            "score": self.score,
            "checks": self.checks,
            "details": self.details,
            "engine_check": self.engine_check,
        }


def _extract_block_names(config: str, section: str) -> list[str]:
    """Return connector block names appearing inside a top-level section.

    Reuses the same brace-walking approach as agents._extract_connector_blocks_raw
    but also supports the transform section.
    """
    m = re.search(rf"(?:^|\n)\s*{section}\s*\{{", config, re.IGNORECASE)
    if not m:
        return []
    start = m.end() - 1
    depth = 0
    end = start
    for i in range(start, len(config)):
        if config[i] == "{":
            depth += 1
        elif config[i] == "}":
            depth -= 1
            if depth == 0:
                end = i
                break
    body = config[start + 1 : end]

    names = []
    pos = 0
    pattern = re.compile(r"([\w][\w-]*)\s*\{")
    while pos < len(body):
        cm = pattern.search(body, pos)
        if not cm:
            break
        names.append(cm.group(1))
        # skip past this block
        bdepth = 0
        j = cm.end() - 1
        for j in range(cm.end() - 1, len(body)):
            if body[j] == "{":
                bdepth += 1
            elif body[j] == "}":
                bdepth -= 1
                if bdepth == 0:
                    break
        pos = j + 1
    return names


def _job_mode(config: str) -> str:
    m = re.search(r"job\.mode\s*=\s*\"?(\w+)\"?", config)
    return m.group(1).upper() if m else ""


def _connector_match(expected: list[str], actual: list[str]) -> bool:
    """Every expected connector name appears among actual blocks (case-insensitive)."""
    actual_lower = {a.lower() for a in actual}
    return all(e.lower() in actual_lower for e in expected)


def score_task(task: dict, config: str | None, run_engine_check: bool = False) -> TaskScore:
    """Score a single generated config against a task's expectations."""
    ts = TaskScore(task_id=task["id"])
    expect = task.get("expect", {})

    if not config or not config.strip():
        for name in WEIGHTS:
            ts.checks[name] = False
        ts.details["error"] = "no config generated"
        return ts

    # 1. Parse success + 2. field completeness — via local validator
    validation = validate_hocon(config)
    parse_errors = [
        line for line in validation.splitlines()
        if line.startswith("ERROR:") and "missing required" not in line
    ]
    missing_required = [
        line for line in validation.splitlines()
        if line.startswith("ERROR:") and "missing required" in line
    ]
    ts.checks["parse_success"] = not parse_errors
    if parse_errors:
        ts.details["parse_success"] = "; ".join(parse_errors)[:500]
    ts.checks["field_completeness"] = not missing_required
    if missing_required:
        ts.details["field_completeness"] = "; ".join(missing_required)[:500]

    # 3. Connector match
    sources = _extract_block_names(config, "source")
    sinks = _extract_block_names(config, "sink")
    transforms = _extract_block_names(config, "transform")
    ok = _connector_match(expect.get("source", []), sources) and \
        _connector_match(expect.get("sink", []), sinks)
    if expect.get("transform"):
        ok = ok and _connector_match(expect["transform"], transforms)
    ts.checks["connector_match"] = ok
    ts.details["connector_match"] = (
        f"source={sources} sink={sinks} transform={transforms}"
    )

    # 4. Job mode
    expected_mode = expect.get("job_mode", "").upper()
    actual_mode = _job_mode(config)
    ts.checks["job_mode_match"] = (not expected_mode) or actual_mode == expected_mode
    ts.details["job_mode_match"] = f"expected={expected_mode} actual={actual_mode}"

    # 5. Content regexes
    content_ok = True
    misses = []
    for pattern in expect.get("must_match", []):
        if not re.search(pattern, config):
            content_ok = False
            misses.append(f"missing:{pattern}")
    for pattern in expect.get("must_not_match", []):
        if re.search(pattern, config):
            content_ok = False
            misses.append(f"forbidden:{pattern}")
    ts.checks["content_match"] = content_ok
    if misses:
        ts.details["content_match"] = "; ".join(misses)[:500]

    # 6. Optional engine-level check
    if run_engine_check:
        dryrun = dry_run_config(config)
        ts.engine_check = dryrun.get("phase2_check")

    return ts

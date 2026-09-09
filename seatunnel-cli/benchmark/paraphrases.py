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

"""Public alternative wording, pinned to reviewed baseline task contracts."""

import copy
import hashlib
import json
from pathlib import Path

PARAPHRASES_PATH = Path(__file__).resolve().parent / "tasks" / "paraphrase.json"


def load_paraphrases(parents: list[dict]) -> list[dict]:
    """Inherit a reviewed task without silently accepting contract drift."""
    by_id = {task["id"]: task for task in parents}
    if len(by_id) != len(parents):
        raise ValueError("Duplicate baseline task IDs")
    data = json.loads(PARAPHRASES_PATH.read_text(encoding="utf-8"))
    variants = data.get("paraphrases") if isinstance(data, dict) else None
    if not isinstance(variants, list) or not variants:
        raise ValueError("Expected a nonempty paraphrases list")
    tasks = []
    seen = set(by_id)
    for variant in variants:
        if not isinstance(variant, dict) or set(variant) != {
            "parent_id",
            "parent_sha256",
            "prompt",
        }:
            raise ValueError(
                "Paraphrases may only specify parent_id, parent_sha256 and prompt"
            )
        parent_id = variant["parent_id"]
        if not isinstance(parent_id, str) or parent_id not in by_id:
            raise ValueError(f"Unknown paraphrase parent: {parent_id!r}")
        task_id = parent_id + "_p1"
        if task_id in seen:
            raise ValueError(f"Duplicate paraphrase task ID: {task_id}")
        seen.add(task_id)
        parent = by_id[parent_id]
        fingerprint = hashlib.sha256(
            json.dumps(
                parent, sort_keys=True, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8")
        ).hexdigest()
        if variant["parent_sha256"] != fingerprint:
            raise ValueError(f"Paraphrase parent changed; review and repin {parent_id}")
        prompt = variant["prompt"]
        if (
            not isinstance(prompt, str)
            or not prompt.strip()
            or prompt.strip() == parent["prompt"].strip()
        ):
            raise ValueError(f"Paraphrase needs distinct, nonempty wording: {task_id}")
        task = copy.deepcopy(parent)
        task.update(variant, id=task_id)
        tasks.append(task)
    return tasks

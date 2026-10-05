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

"""Checks on the planner system prompt itself.

Whether the planner routes a given request to PLAN or CHAT is a model
behaviour and is measured by the benchmark, not here. What is checkable
offline is that the few-shot examples teach a well-formed plan: a malformed
JSON example would silently train the model to emit malformed JSON, and
nothing downstream would point back at the prompt as the cause.
"""

import json
import re

from seatunnel_cli.agents import PLANNER_SYSTEM


# "User: ...\nResponse:\nPLAN:" or "...CHAT:" -- the label the example teaches.
_EXAMPLE_RE = re.compile(
    r'User: "(?P<prompt>.*?)"\nResponse:\n(?P<label>PLAN|CHAT):',
    re.DOTALL,
)

# The ```json ... ``` body following a PLAN: label.
_PLAN_JSON_RE = re.compile(r"PLAN:\n```json\n(?P<body>.*?)\n```", re.DOTALL)


def _few_shot_section():
    # Only the examples. The "Output Format" section above them holds schema
    # templates with placeholders ("port": <port>), which are deliberately not
    # valid JSON and must not be checked as if they were.
    return PLANNER_SYSTEM[PLANNER_SYSTEM.index("## Few-shot Examples"):]


def _examples():
    return _EXAMPLE_RE.findall(_few_shot_section())


def test_both_labels_are_demonstrated():
    labels = [label for _, label in _examples()]
    assert labels.count("PLAN") >= 3
    assert labels.count("CHAT") >= 3


def test_plan_examples_are_valid_json_matching_the_documented_shape():
    bodies = _PLAN_JSON_RE.findall(_few_shot_section())
    assert bodies, "no PLAN examples found"
    for body in bodies:
        plan = json.loads(body)
        assert plan["pipelines"], "a plan must contain at least one pipeline"
        for pipeline in plan["pipelines"]:
            # Keys the config generator reads; a typo here is invisible until
            # generation fails on live traffic.
            for key in ("id", "source", "sink", "transform", "tables"):
                assert key in pipeline, f"{key} missing from {pipeline.get('id')}"
            for end in ("source", "sink"):
                assert pipeline[end]["connector"]
                assert pipeline[end]["reason"]
        assert plan["env"]["mode"] in ("BATCH", "STREAMING")
        assert isinstance(plan["env"]["parallelism"], int)


def test_classification_is_not_stated_as_a_closed_verb_list():
    # The rule the routing fix rests on: classify on the outcome asked for,
    # never on whether the request used a verb from some list. Reintroducing a
    # closed list is what previously dropped "export"/"print" requests to CHAT.
    section = PLANNER_SYSTEM[
        PLANNER_SYSTEM.index("## How to Classify User Intent"):
        PLANNER_SYSTEM.index("## Default Assumptions")
    ]
    assert "never exhaustive" in section
    assert "Tie-breakers" in section
    # Verbs whose absence caused real misroutes must stay represented.
    for verb in ("export", "print", "stream", "capture", "build"):
        assert verb in section.lower(), verb
    # Diagnosis must still route to CHAT -- the diagnostics work depends on it.
    assert "stack traces" in section

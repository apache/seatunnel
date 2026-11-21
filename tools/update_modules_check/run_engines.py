#!/usr/bin/env python3
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""Decide which engines the backend integration test jobs have to cover.

The backend workflow runs the integration test suites against the Flink, Spark and Zeta
containers. Every job passes the resulting list to the tests through the ``RUN_ENGINES``
environment variable, so a job only starts the containers of the engines a change can
affect:

* shared code, connector modules, the distribution and the standalone modules are used by
  every engine, so they keep the full matrix;
* ``seatunnel-translation`` implements the plan translators of the Flink and Spark
  starters only, so it never needs the Zeta container: the base module is shared by both,
  while ``seatunnel-translation-flink`` / ``-spark`` only need their own engine;
* ``seatunnel-engine`` is the Zeta engine and is not used by the Flink and Spark starters;
* when no rule matches, every engine runs: coverage is only narrowed on purpose.
"""

import argparse
import json
from typing import Iterable, List


ALL_ENGINES = ("flink", "spark", "zeta")

# Engines a path group can affect. Keep this in sync with the globs of the `changes` job in
# .github/workflows/backend.yml.
GROUP_ENGINES = {
    "shared": ALL_ENGINES,
    "connectors": ALL_ENGINES,
    "connector-e2e": ALL_ENGINES,
    "dist": ALL_ENGINES,
    "standalone": ALL_ENGINES,
    "translation-base": ("flink", "spark"),
    "translation-flink": ("flink",),
    "translation-spark": ("spark",),
    "engine": ("zeta",),
    "engine-e2e": ("zeta",),
}


def run_engines_for_groups(changed_groups: Iterable[str]) -> List[str]:
    """Return the engines to run for the changed path groups."""

    engines = set()
    for group in changed_groups:
        engines.update(GROUP_ENGINES[group])
    if not engines:
        return list(ALL_ENGINES)
    return [engine for engine in ALL_ENGINES if engine in engines]


def parse_bool(value: str) -> bool:
    """Parse the shell-friendly booleans emitted by the workflow."""

    if value == "true":
        return True
    if value == "false":
        return False
    raise ValueError(f"invalid boolean: {value}")


def main() -> None:
    """Print the JSON list of engines for the workflow caller."""

    parser = argparse.ArgumentParser(
        description="Decide which engines the backend integration test jobs have to cover."
    )
    for group in GROUP_ENGINES:
        parser.add_argument(
            f"--{group}",
            choices=("true", "false"),
            default="false",
            help=f"Whether the `{group}` path group changed",
        )
    args = parser.parse_args()

    changed_groups = [
        group for group in GROUP_ENGINES if parse_bool(getattr(args, group.replace("-", "_")))
    ]
    print(json.dumps(run_engines_for_groups(changed_groups), separators=(",", ":")))


if __name__ == "__main__":
    main()

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

"""Helpers for deciding when the backend workflow must keep full API coverage."""

import argparse
import json
import re
from typing import Iterable


PROTECTED_BRANCH_NAMES = {"dev", "main", "master"}
RELEASE_BRANCH_PATTERN = re.compile(r"\d+\.\d+(?:\.\d+)?-release")
LIGHTWEIGHT_API_FILE_PREFIXES = (
    ".github/workflows/",
    "seatunnel-dist/",
    "tools/benchmarks/",
    "tools/update_modules_check/",
)
LIGHTWEIGHT_API_FILES = {"bin/install-plugin.sh"}


def should_force_full_api_check(repository_owner: str, github_ref: str) -> bool:
    """Return whether the backend workflow should bypass incremental CI scoping."""

    if repository_owner != "apache":
        return False

    ref_prefix = "refs/heads/"
    if github_ref.startswith(ref_prefix):
        ref_name = github_ref[len(ref_prefix) :]
    elif github_ref.startswith("refs/"):
        return False
    else:
        # GITHUB_BASE_REF is a bare branch name on pull_request events.
        ref_name = github_ref

    return (
        ref_name in PROTECTED_BRANCH_NAMES
        or RELEASE_BRANCH_PATTERN.fullmatch(ref_name) is not None
    )


def is_lightweight_api_file(path: str) -> bool:
    """Return true when a broad API glob matched a file that has a cheaper check path."""

    return path in LIGHTWEIGHT_API_FILES or any(
        path.startswith(prefix) for prefix in LIGHTWEIGHT_API_FILE_PREFIXES
    )


def should_run_full_api_check(
    repository_owner: str,
    github_ref: str,
    api_changed: bool,
    api_changed_files: Iterable[str],
) -> bool:
    """Return whether the backend workflow should run the full API-triggered matrix."""

    if should_force_full_api_check(repository_owner, github_ref):
        return True

    if not api_changed:
        return False

    return any(not is_lightweight_api_file(path) for path in api_changed_files)


def parse_bool(value: str) -> bool:
    """Parse the shell-friendly booleans emitted by check_file_updates.py."""

    if value == "true":
        return True
    if value == "false":
        return False
    raise ValueError(f"invalid boolean: {value}")


def parse_changed_files(value: str) -> list[str]:
    """Parse a JSON list of changed file paths."""

    parsed = json.loads(value)
    if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
        raise ValueError("api files must be a JSON array of strings")
    return parsed


def main() -> None:
    """Print a shell-friendly boolean for the workflow caller."""

    parser = argparse.ArgumentParser(
        description="Decide whether the backend workflow should force full API coverage."
    )
    parser.add_argument("repository_owner", help="Value of GITHUB_REPOSITORY_OWNER")
    parser.add_argument("github_ref", help="Value of GITHUB_BASE_REF or GITHUB_REF")
    parser.add_argument(
        "--api-changed",
        choices=("true", "false"),
        help="Whether API-scoped globs matched changed files",
    )
    parser.add_argument(
        "--api-files-json",
        default="[]",
        help="JSON array emitted by check_file_updates.py for API-scoped globs",
    )
    args = parser.parse_args()

    if args.api_changed is None:
        result = should_force_full_api_check(args.repository_owner, args.github_ref)
    else:
        result = should_run_full_api_check(
            args.repository_owner,
            args.github_ref,
            parse_bool(args.api_changed),
            parse_changed_files(args.api_files_json),
        )

    print("true" if result else "false")


if __name__ == "__main__":
    main()

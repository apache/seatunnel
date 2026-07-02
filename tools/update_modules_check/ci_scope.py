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


PROTECTED_BRANCH_NAMES = {"dev", "main", "master"}


def should_force_full_api_check(repository_owner: str, github_ref: str) -> bool:
    """Return whether the backend workflow should bypass incremental CI scoping.

    Full API coverage remains mandatory on protected long-lived apache branches because those
    pushes directly affect shared integration baselines. Feature and PR branches should keep the
    changed-file scope so they do not fan out into the full matrix on every update.
    """

    if repository_owner != "apache":
        return False

    ref_prefix = "refs/heads/"
    ref_name = github_ref[len(ref_prefix) :] if github_ref.startswith(ref_prefix) else github_ref
    return ref_name in PROTECTED_BRANCH_NAMES or ref_name.endswith("-release")


def main() -> None:
    """Print a shell-friendly boolean for the workflow caller."""

    parser = argparse.ArgumentParser(
        description="Decide whether the backend workflow should force full API coverage."
    )
    parser.add_argument("repository_owner", help="Value of GITHUB_REPOSITORY_OWNER")
    parser.add_argument("github_ref", help="Value of GITHUB_REF")
    args = parser.parse_args()

    print("true" if should_force_full_api_check(args.repository_owner, args.github_ref) else "false")


if __name__ == "__main__":
    main()

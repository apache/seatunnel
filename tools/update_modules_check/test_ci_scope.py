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

"""Regression tests for backend workflow CI scoping decisions."""

import unittest

from ci_scope import should_force_full_api_check, should_run_full_api_check


class CiScopeTest(unittest.TestCase):
    """Verify that heavyweight CI only runs when a change really needs it."""

    def test_force_full_api_check_on_dev_branch(self) -> None:
        self.assertTrue(should_force_full_api_check("apache", "refs/heads/dev"))

    def test_force_full_api_check_on_main_and_master_branches(self) -> None:
        for branch_name in ("main", "master"):
            with self.subTest(branch_name=branch_name):
                self.assertTrue(
                    should_force_full_api_check("apache", f"refs/heads/{branch_name}")
                )

    def test_force_full_api_check_on_release_branch(self) -> None:
        self.assertTrue(should_force_full_api_check("apache", "refs/heads/2.3.13-release"))

    def test_force_full_api_check_on_two_component_release_branch(self) -> None:
        self.assertTrue(should_force_full_api_check("apache", "refs/heads/2.4-release"))

    def test_force_full_api_check_on_pull_request_base_branch(self) -> None:
        self.assertTrue(should_force_full_api_check("apache", "dev"))

    def test_skip_force_full_api_check_for_fork_owner(self) -> None:
        self.assertFalse(should_force_full_api_check("DanielLeens", "refs/heads/dev"))

    def test_skip_force_full_api_check_on_pull_request_merge_ref(self) -> None:
        self.assertFalse(should_force_full_api_check("apache", "refs/pull/123/merge"))

    def test_skip_force_full_api_check_on_non_version_release_branch(self) -> None:
        self.assertFalse(
            should_force_full_api_check("apache", "refs/heads/hotfix-release")
        )

    def test_skip_force_full_api_check_on_release_tag(self) -> None:
        self.assertFalse(
            should_force_full_api_check("apache", "refs/tags/2.3.13-release")
        )

    def test_skip_full_api_check_for_ci_only_fork_change(self) -> None:
        self.assertFalse(
            should_run_full_api_check(
                "DanielLeens",
                "refs/heads/ci-scope",
                True,
                [
                    ".github/workflows/backend.yml",
                    "tools/update_modules_check/ci_scope.py",
                ],
            )
        )

    def test_skip_full_api_check_for_benchmark_tools_change(self) -> None:
        self.assertFalse(
            should_run_full_api_check(
                "DanielLeens",
                "refs/heads/benchmark-change",
                True,
                ["tools/benchmarks/save_jmh_result.py"],
            )
        )

    def test_skip_full_api_check_for_dist_only_fork_change(self) -> None:
        self.assertFalse(
            should_run_full_api_check(
                "DanielLeens",
                "refs/heads/install-plugin",
                True,
                [
                    "bin/install-plugin.sh",
                    "seatunnel-dist/src/test/java/"
                    "org/apache/seatunnel/installer/InstallPluginScriptTest.java",
                ],
            )
        )

    def test_run_full_api_check_for_source_api_change(self) -> None:
        self.assertTrue(
            should_run_full_api_check(
                "DanielLeens",
                "refs/heads/api-change",
                True,
                [
                    "seatunnel-api/src/main/java/"
                    "org/apache/seatunnel/api/table/type/SeaTunnelRow.java"
                ],
            )
        )

    def test_run_full_api_check_for_mixed_lightweight_and_source_change(self) -> None:
        self.assertTrue(
            should_run_full_api_check(
                "DanielLeens",
                "refs/heads/mixed-change",
                True,
                [
                    ".github/workflows/backend.yml",
                    "seatunnel-core/seatunnel-starter/src/main/java/"
                    "org/apache/seatunnel/core/starter/SeaTunnel.java",
                ],
            )
        )

    def test_protected_branch_keeps_full_api_for_lightweight_change(self) -> None:
        self.assertTrue(
            should_run_full_api_check(
                "apache",
                "refs/heads/dev",
                True,
                [".github/workflows/backend.yml"],
            )
        )


if __name__ == "__main__":
    unittest.main()

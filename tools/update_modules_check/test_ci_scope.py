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

from ci_scope import should_force_full_api_check


class CiScopeTest(unittest.TestCase):
    """Verify that only protected apache branches keep unconditional full CI."""

    def test_force_full_api_check_on_dev_branch(self) -> None:
        self.assertTrue(should_force_full_api_check("apache", "refs/heads/dev"))

    def test_force_full_api_check_on_release_branch(self) -> None:
        self.assertTrue(should_force_full_api_check("apache", "refs/heads/2.3.13-release"))

    def test_skip_force_full_api_check_on_feature_branch(self) -> None:
        self.assertFalse(
            should_force_full_api_check(
                "apache", "refs/heads/dev-pulsar-cleanup-preload-20260623"
            )
        )

    def test_skip_force_full_api_check_on_docs_branch(self) -> None:
        self.assertFalse(
            should_force_full_api_check("apache", "refs/heads/docs/e2e-sls-docs-20260703")
        )

    def test_skip_force_full_api_check_for_fork_owner(self) -> None:
        self.assertFalse(should_force_full_api_check("DanielLeens", "refs/heads/dev"))


if __name__ == "__main__":
    unittest.main()

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

"""Planner output is free text, so a connector name arrives in any spelling.

`jdbc`, `MYSQL-CDC` and `SQLTransform` all name connectors whose metadata
exists, but the lookup was exact, so it logged "No metadata found" and the
generator was left to invent option keys for them.

Resolution stays limited to spellings of the same name. It must not guess at
abbreviations: 13 registered connectors end in `File`, so resolving a bare
`File` to one of them would point a job at the wrong storage system.
"""

import logging
import unittest
from unittest import mock

from seatunnel_cli.connectors import canonicalize_connector, fetch_connector_metadata


# Mirrors the shape of list_connector_names(). The File family is listed in
# full because it is what makes a bare `File` ambiguous; the names are taken
# from plugin-mapping.properties in this repo.
CATALOG = {
    "sources": ["Jdbc", "MySQL-CDC", "FakeSource", "Kafka", "LocalFile", "S3File",
                "OssFile", "HdfsFile", "SftpFile"],
    "sinks": ["Jdbc", "Console", "Assert", "LocalFile", "S3File", "OssFile",
              "HdfsFile", "SftpFile", "Clickhouse"],
    "transforms": ["Sql", "Filter", "FieldMapper"],
}


class CanonicalizeConnectorTest(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch(
            "seatunnel_cli.connectors.list_connector_names", return_value=CATALOG,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_a_registered_name_resolves_to_itself(self):
        self.assertEqual(canonicalize_connector("Jdbc", "source"), ("Jdbc", []))

    def test_case_only_differences_resolve(self):
        for given in ("jdbc", "JDBC", "jDbC"):
            with self.subTest(given=given):
                self.assertEqual(canonicalize_connector(given, "source"), ("Jdbc", []))

    def test_separator_differences_resolve(self):
        for given in ("mysql-cdc", "MYSQL_CDC", "MySqlCdc", "mysqlcdc"):
            with self.subTest(given=given):
                self.assertEqual(
                    canonicalize_connector(given, "source"), ("MySQL-CDC", []),
                )

    def test_a_transform_with_its_kind_spelled_out_resolves(self):
        self.assertEqual(canonicalize_connector("SQLTransform", "transform"), ("Sql", []))

    def test_an_ambiguous_abbreviation_is_not_guessed(self):
        """`File` must not become `LocalFile`.

        13 registered connectors end in File. Picking one would send the job to
        the wrong storage system, which is worse than the warning it replaces,
        so the candidates are returned instead.
        """
        canonical, candidates = canonicalize_connector("File", "sink")

        self.assertIsNone(canonical)
        self.assertIn("LocalFile", candidates)
        self.assertIn("S3File", candidates)
        self.assertIn("OssFile", candidates)
        self.assertGreater(len(candidates), 1)

    def test_a_name_registered_for_both_types_is_not_ambiguous(self):
        """`Jdbc` is both a source and a sink, which is one connector."""
        self.assertEqual(canonicalize_connector("jdbc", "sink"), ("Jdbc", []))

    def test_the_type_scopes_the_search(self):
        """A sink-only name should not resolve while asking for a source."""
        canonical, _candidates = canonicalize_connector("clickhouse", "source")

        self.assertIsNone(canonical)

    def test_an_unknown_name_resolves_to_nothing(self):
        self.assertEqual(canonicalize_connector("Snowflake", "sink"), (None, []))

    def test_a_name_registered_under_two_types_is_not_ambiguous_with_itself(self):
        """Without a type, `Jdbc` is reachable as both a source and a sink.

        Found by mutation-testing this function: before the de-duplication the
        unscoped lookup returned `(None, ['Jdbc', 'Jdbc'])` and refused to
        resolve a name that is one connector.
        """
        self.assertEqual(canonicalize_connector("jdbc"), ("Jdbc", []))

    def test_two_distinct_names_spelled_alike_return_candidates(self):
        """The one case where a normalised key really is ambiguous.

        Two different registered connectors reducing to the same key must come
        back as candidates rather than having one of them picked.
        """
        with mock.patch(
            "seatunnel_cli.connectors.list_connector_names",
            return_value={"sources": [], "sinks": ["Oss-File", "OssFile"],
                          "transforms": []},
        ):
            canonical, candidates = canonicalize_connector("ossfile", "sink")

        self.assertIsNone(canonical)
        self.assertEqual(candidates, ["Oss-File", "OssFile"])

    def test_an_unavailable_registry_is_not_fatal(self):
        with mock.patch(
            "seatunnel_cli.connectors.list_connector_names",
            side_effect=RuntimeError("no metadata"),
        ):
            self.assertEqual(canonicalize_connector("jdbc", "source"), (None, []))


class FetchFallsBackToTheCanonicalNameTest(unittest.TestCase):
    def setUp(self):
        for target, kwargs in (
            ("seatunnel_cli.connectors.list_connector_names", {"return_value": CATALOG}),
            ("seatunnel_cli.connectors._fetch_option_rules", {"return_value": None}),
        ):
            patcher = mock.patch(target, **kwargs)
            patcher.start()
            self.addCleanup(patcher.stop)

    @staticmethod
    def _runtime(*keys):
        return {
            key: {"name": key.split(":", 1)[1], "type": key.split(":", 1)[0],
                  "required": [], "optional": [], "conditional": []}
            for key in keys
        }

    def test_a_differently_spelled_name_finds_its_metadata(self):
        with mock.patch(
            "seatunnel_cli.connectors._load_runtime_metadata",
            return_value=self._runtime("source:Jdbc"),
        ):
            detail = fetch_connector_metadata("jdbc", "source")

        self.assertIsNotNone(detail)
        self.assertEqual(detail["name"], "Jdbc")

    def test_a_name_that_already_matches_is_not_rewritten(self):
        """The fallback runs only after the name as given has failed.

        A lookup that worked before must take exactly the path it took before.
        """
        with mock.patch(
            "seatunnel_cli.connectors._load_runtime_metadata",
            return_value=self._runtime("source:Jdbc"),
        ), mock.patch(
            "seatunnel_cli.connectors.canonicalize_connector"
        ) as canonicalize:
            detail = fetch_connector_metadata("Jdbc", "source")

        self.assertIsNotNone(detail)
        canonicalize.assert_not_called()

    def test_an_ambiguous_name_reports_the_candidates(self):
        with mock.patch(
            "seatunnel_cli.connectors._load_runtime_metadata",
            return_value=self._runtime("sink:LocalFile", "sink:S3File"),
        ), self.assertLogs("seatunnel_cli.connectors", level=logging.WARNING) as logs:
            detail = fetch_connector_metadata("File", "sink")

        self.assertIsNone(detail)
        joined = "\n".join(logs.output)
        self.assertIn("could mean", joined)
        self.assertIn("LocalFile", joined)
        self.assertIn("S3File", joined)

    def test_an_unknown_name_still_reports_plainly(self):
        with mock.patch(
            "seatunnel_cli.connectors._load_runtime_metadata",
            return_value=self._runtime("sink:Console"),
        ), self.assertLogs("seatunnel_cli.connectors", level=logging.WARNING) as logs:
            detail = fetch_connector_metadata("Snowflake", "sink")

        self.assertIsNone(detail)
        self.assertIn("No metadata found for sink:Snowflake", "\n".join(logs.output))


if __name__ == "__main__":
    unittest.main()

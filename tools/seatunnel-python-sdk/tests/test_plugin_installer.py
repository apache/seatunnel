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


import os
import subprocess
import tempfile
import unittest
from unittest import mock

from seatunnel.plugin_installer import PluginInstallError, install_plugins, select_plugins


# Same layout as config/plugin_config in the distribution: license header, a description
# comment, a section header and one connector artifact ID per line.
PLUGIN_CONFIG = (
    "#\n"
    "# Licensed to the Apache Software Foundation (ASF) under one or more\n"
    "#    http://www.apache.org/licenses/LICENSE-2.0\n"
    "#\n"
    "# Don't modify the delimiter \" -- \", just select the plugin you need\n"
    "--connectors-v2--\n"
    "connector-fake\n"
    "connector-console\n"
    "connector-jdbc\n"
    "connector-cdc-mysql\n"
    "connector-kafka\n"
)


class _PluginConfigTestCase(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.home = self._tmp.name
        os.makedirs(os.path.join(self.home, "config"))
        os.makedirs(os.path.join(self.home, "bin"))
        self.config_path = os.path.join(self.home, "config", "plugin_config")
        self.script_path = os.path.join(self.home, "bin", "install-plugin.sh")
        self._write(PLUGIN_CONFIG)
        with open(self.script_path, "w") as f:
            f.write("#!/bin/bash\n")

    def tearDown(self):
        self._tmp.cleanup()

    def _write(self, content):
        with open(self.config_path, "w", encoding="utf-8", newline="") as f:
            f.write(content)

    def _read(self):
        with open(self.config_path, encoding="utf-8", newline="") as f:
            return f.read()

    def _enabled(self):
        """Connector lines that install-plugin.sh would install (not '#' or '-', not blank)."""
        return [
            line for line in self._read().splitlines()
            if line and line[0] not in "#-"
        ]


class TestSelectPlugins(_PluginConfigTestCase):

    def test_only_requested_connectors_stay_enabled(self):
        """Requested connectors stay enabled; the others are disabled with '#'."""
        selected = select_plugins(self.config_path, ["connector-jdbc", "connector-cdc-mysql"])

        self.assertEqual(["connector-jdbc", "connector-cdc-mysql"], selected)
        self.assertEqual(["connector-jdbc", "connector-cdc-mysql"], self._enabled())
        content = self._read()
        for disabled in ("#connector-fake", "#connector-console", "#connector-kafka"):
            self.assertIn(disabled + "\n", content)

    def test_header_comments_and_section_header_are_preserved(self):
        """License header, description comment and section header are unchanged."""
        select_plugins(self.config_path, ["jdbc"])

        lines = self._read().splitlines()
        self.assertEqual(PLUGIN_CONFIG.splitlines()[:6], lines[:6])
        self.assertIn("--connectors-v2--", lines)

    def test_second_run_with_same_connectors_is_idempotent(self):
        """Running twice with the same request produces byte-identical content."""
        select_plugins(self.config_path, ["jdbc", "cdc-mysql"])
        first = self._read()

        select_plugins(self.config_path, ["jdbc", "cdc-mysql"])
        self.assertEqual(first, self._read())

    def test_unchanged_file_is_not_rewritten(self):
        """When nothing changes, the file is not written again."""
        select_plugins(self.config_path, ["jdbc"])

        with mock.patch("seatunnel.plugin_installer._write_atomically") as write:
            select_plugins(self.config_path, ["jdbc"])
        write.assert_not_called()

    def test_previously_disabled_connector_can_be_enabled_again(self):
        """A connector disabled by an earlier call can be selected by a later one."""
        select_plugins(self.config_path, ["jdbc"])
        select_plugins(self.config_path, ["kafka"])

        self.assertEqual(["connector-kafka"], self._enabled())
        self.assertIn("#connector-jdbc\n", self._read())

    def test_short_name_and_artifact_id_are_equivalent(self):
        """'jdbc' and 'connector-jdbc' select the same connector."""
        self.assertEqual(["connector-jdbc"], select_plugins(self.config_path, ["jdbc"]))
        short = self._read()
        self._write(PLUGIN_CONFIG)
        self.assertEqual(["connector-jdbc"], select_plugins(self.config_path, ["connector-jdbc"]))
        self.assertEqual(short, self._read())

    def test_duplicate_names_are_collapsed(self):
        """Duplicates, including short and full forms of one name, appear once."""
        selected = select_plugins(self.config_path, ["jdbc", "connector-jdbc", "jdbc"])
        self.assertEqual(["connector-jdbc"], selected)

    def test_single_string_is_accepted(self):
        """A single connector name may be passed as a plain string."""
        self.assertEqual(["connector-jdbc"], select_plugins(self.config_path, "jdbc"))

    def test_unknown_connector_raises_and_leaves_file_unchanged(self):
        """A connector missing from plugin_config raises and does not modify the file."""
        with self.assertRaises(PluginInstallError) as ctx:
            select_plugins(self.config_path, ["jdbc", "does-not-exist"])

        self.assertIn("connector-does-not-exist", str(ctx.exception))
        self.assertNotIn("connector-jdbc,", str(ctx.exception))
        self.assertEqual(PLUGIN_CONFIG, self._read())

    def test_empty_request_raises(self):
        """An empty connector list raises instead of disabling every connector."""
        with self.assertRaises(PluginInstallError):
            select_plugins(self.config_path, [])
        self.assertEqual(PLUGIN_CONFIG, self._read())

    def test_invalid_connector_names_raise(self):
        """Names with characters install-plugin.sh rejects, or a leading '-', raise."""
        for name in ("", " ", "jdbc mysql", "../jdbc", "jdbc;rm", "--connectors-v2--"):
            with self.subTest(name=name):
                with self.assertRaises(PluginInstallError):
                    select_plugins(self.config_path, [name])
        self.assertEqual(PLUGIN_CONFIG, self._read())

    def test_crlf_line_endings_are_preserved(self):
        """A file with Windows line endings keeps them on every line."""
        self._write(PLUGIN_CONFIG.replace("\n", "\r\n"))

        select_plugins(self.config_path, ["jdbc"])

        content = self._read()
        self.assertNotIn("\n", content.replace("\r\n", ""))
        self.assertIn("#connector-fake\r\n", content)
        self.assertIn("connector-jdbc\r\n", content)

    def test_last_line_without_newline_is_handled(self):
        """A final connector line with no trailing newline is still matched."""
        self._write(PLUGIN_CONFIG.rstrip("\n"))

        select_plugins(self.config_path, ["kafka"])

        self.assertTrue(self._read().endswith("\nconnector-kafka"))
        self.assertEqual(["connector-kafka"], self._enabled())

    def test_missing_file_raises(self):
        """A missing plugin_config raises PluginInstallError."""
        with self.assertRaises(PluginInstallError) as ctx:
            select_plugins(os.path.join(self.home, "nope"), ["jdbc"])
        self.assertIn("Cannot read plugin config", str(ctx.exception))


class TestInstallPlugins(_PluginConfigTestCase):

    def _patch_which(self, path="/usr/bin/bash"):
        return mock.patch("seatunnel.plugin_installer.shutil.which", return_value=path)

    def _patch_run(self, returncode=0, side_effect=None):
        return mock.patch(
            "seatunnel.plugin_installer.subprocess.run",
            return_value=subprocess.CompletedProcess(args=[], returncode=returncode),
            side_effect=side_effect,
        )

    def test_runs_install_script_with_version_after_selecting(self):
        """The config is rewritten first, then install-plugin.sh runs with the version."""
        observed = {}

        def fake_run(args, cwd, check):
            observed["enabled"] = self._enabled()
            return subprocess.CompletedProcess(args=args, returncode=0)

        with self._patch_which(), self._patch_run(side_effect=fake_run) as run:
            installed = install_plugins(self.home, ["fake", "console"], "3.0.1")

        self.assertEqual(["connector-fake", "connector-console"], installed)
        self.assertEqual(["connector-fake", "connector-console"], observed["enabled"])
        run.assert_called_once_with(
            ["/usr/bin/bash", self.script_path, "3.0.1"], cwd=self.home, check=False
        )

    def test_successful_install_keeps_new_selection(self):
        """After a successful install, plugin_config keeps the requested selection."""
        with self._patch_which(), self._patch_run(returncode=0):
            install_plugins(self.home, ["jdbc"], "3.0.1")
        self.assertEqual(["connector-jdbc"], self._enabled())

    def test_nonzero_exit_raises_and_restores_plugin_config(self):
        """A failing install script raises and plugin_config is restored byte for byte."""
        with self._patch_which(), self._patch_run(returncode=1):
            with self.assertRaises(PluginInstallError) as ctx:
                install_plugins(self.home, ["jdbc"], "3.0.1")
        self.assertIn("exited with code 1", str(ctx.exception))
        self.assertIn("connector-jdbc", str(ctx.exception))
        self.assertEqual(PLUGIN_CONFIG, self._read())

    def test_failed_install_restores_previous_selection_and_line_endings(self):
        """Restoring returns the file to its state before the call, not to the default list."""
        self._write(PLUGIN_CONFIG.replace("\n", "\r\n"))
        select_plugins(self.config_path, ["kafka"])
        with open(self.config_path, "rb") as f:
            before = f.read()

        with self._patch_which(), self._patch_run(returncode=1):
            with self.assertRaises(PluginInstallError):
                install_plugins(self.home, ["jdbc"], "3.0.1")

        with open(self.config_path, "rb") as f:
            self.assertEqual(before, f.read())

    def test_oserror_raises_and_restores_plugin_config(self):
        """An OSError starting the script is wrapped and plugin_config is restored."""
        with self._patch_which(), self._patch_run(side_effect=OSError("exec format error")):
            with self.assertRaises(PluginInstallError) as ctx:
                install_plugins(self.home, ["jdbc"], "3.0.1")
        self.assertIn("exec format error", str(ctx.exception))
        self.assertIsInstance(ctx.exception.__cause__, OSError)
        self.assertEqual(PLUGIN_CONFIG, self._read())

    def test_interrupt_restores_plugin_config_and_propagates(self):
        """Ctrl+C during the download restores plugin_config and still interrupts."""
        with self._patch_which(), self._patch_run(side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                install_plugins(self.home, ["jdbc"], "3.0.1")
        self.assertEqual(PLUGIN_CONFIG, self._read())

    def test_restore_failure_reports_both_errors(self):
        """If restoring fails too, the error mentions the script failure and the restore failure."""
        from seatunnel import plugin_installer

        real_write = plugin_installer._write_atomically
        calls = []

        def write_then_fail(path, content):
            calls.append(path)
            if len(calls) > 1:
                raise PluginInstallError("disk full")
            real_write(path, content)

        with self._patch_which(), self._patch_run(returncode=1):
            with mock.patch.object(plugin_installer, "_write_atomically", write_then_fail):
                with self.assertRaises(PluginInstallError) as ctx:
                    install_plugins(self.home, ["jdbc"], "3.0.1")
        self.assertIn("exited with code 1", str(ctx.exception))
        self.assertIn("Restoring the original plugin config also failed: disk full",
                      str(ctx.exception))

    def test_missing_bash_raises_before_rewriting(self):
        """Without bash, nothing is rewritten and nothing is run."""
        with self._patch_which(None), self._patch_run() as run:
            with self.assertRaises(PluginInstallError) as ctx:
                install_plugins(self.home, ["jdbc"], "3.0.1")
        self.assertIn("bash is required", str(ctx.exception))
        run.assert_not_called()
        self.assertEqual(PLUGIN_CONFIG, self._read())

    def test_missing_script_raises_before_rewriting(self):
        """A home without bin/install-plugin.sh is rejected before plugin_config changes."""
        os.remove(self.script_path)
        with self._patch_which(), self._patch_run() as run:
            with self.assertRaises(PluginInstallError) as ctx:
                install_plugins(self.home, ["jdbc"], "3.0.1")
        self.assertIn("install-plugin.sh", str(ctx.exception))
        run.assert_not_called()
        self.assertEqual(PLUGIN_CONFIG, self._read())

    def test_missing_plugin_config_raises(self):
        """A home without config/plugin_config is rejected."""
        os.remove(self.config_path)
        with self._patch_which(), self._patch_run() as run:
            with self.assertRaises(PluginInstallError) as ctx:
                install_plugins(self.home, ["jdbc"], "3.0.1")
        self.assertIn("plugin_config", str(ctx.exception))
        run.assert_not_called()

    def test_unknown_connector_does_not_run_script(self):
        """If a connector is not listed, the script is not run."""
        with self._patch_which(), self._patch_run() as run:
            with self.assertRaises(PluginInstallError):
                install_plugins(self.home, ["does-not-exist"], "3.0.1")
        run.assert_not_called()
        self.assertEqual(PLUGIN_CONFIG, self._read())

    def test_empty_version_raises(self):
        """An empty version is rejected before anything is rewritten or run."""
        for version in ("", "  ", None):
            with self.subTest(version=version):
                with self._patch_which(), self._patch_run() as run:
                    with self.assertRaises(PluginInstallError):
                        install_plugins(self.home, ["jdbc"], version)
                run.assert_not_called()
        self.assertEqual(PLUGIN_CONFIG, self._read())


if __name__ == "__main__":
    unittest.main()

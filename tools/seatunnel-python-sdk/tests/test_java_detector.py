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
import sys
import unittest
from unittest import mock

from seatunnel.java_detector import JavaDetectionError, JavaInfo, detect_java


def _make_completed_process(stderr: str = "", stdout: str = "", returncode: int = 0):
    """Build a fake CompletedProcess without running a real subprocess."""
    return subprocess.CompletedProcess(
        args=["java", "-version"],
        returncode=returncode,
        stdout=stdout.encode(),
        stderr=stderr.encode(),
    )


class TestDetectJava(unittest.TestCase):

    # ------------------------------------------------------------------ helpers

    def _patch_which(self, path):
        """Patch shutil.which to return *path*."""
        return mock.patch("seatunnel.java_detector.shutil.which", return_value=path)

    def _patch_run(self, stderr="", stdout="", returncode=0):
        """Patch subprocess.run to return a fake CompletedProcess."""
        return mock.patch(
            "seatunnel.java_detector.subprocess.run",
            return_value=_make_completed_process(stderr=stderr, stdout=stdout, returncode=returncode),
        )

    def _patch_env(self, java_home=None):
        """Patch os.environ so JAVA_HOME is set to *java_home* (or absent if None)."""
        env = {}
        if java_home is not None:
            env["JAVA_HOME"] = java_home
        return mock.patch.dict("os.environ", env, clear=True)

    # -------------------------------------------------------- version-parsing cases

    def test_java_8_old_format_raises(self):
        """'1.8.0_292' must be detected as Java 8 and rejected (< 11)."""
        with self._patch_env(), self._patch_which("/usr/bin/java"):
            with self._patch_run(stderr='openjdk version "1.8.0_292" 2021-04-20'):
                with self.assertRaises(JavaDetectionError) as ctx:
                    detect_java()
        self.assertIn("Java 8", str(ctx.exception))
        self.assertIn("Java 11+", str(ctx.exception))

    def test_java_7_old_format_raises(self):
        """'1.7.0_80' must be detected as Java 7 and rejected."""
        with self._patch_env(), self._patch_which("/usr/bin/java"):
            with self._patch_run(stderr='java version "1.7.0_80"'):
                with self.assertRaises(JavaDetectionError) as ctx:
                    detect_java()
        self.assertIn("Java 7", str(ctx.exception))

    def test_java_11_on_path_returns_info(self):
        """Java 11 on PATH returns correct JavaInfo."""
        with self._patch_env(), self._patch_which("/usr/bin/java"):
            with self._patch_run(stderr='openjdk version "11.0.2" 2019-01-15'):
                info = detect_java()
        self.assertIsInstance(info, JavaInfo)
        self.assertEqual(11, info.major_version)
        self.assertEqual("/usr/bin/java", info.executable)

    def test_java_17_via_java_home_returns_info(self):
        """Java 17 via JAVA_HOME returns correct JavaInfo with JAVA_HOME-based path."""
        fake_home = "/opt/jdk17"
        expected_binary = "java.exe" if sys.platform == "win32" else "java"
        expected_exe = os.path.join(fake_home, "bin", expected_binary)

        with self._patch_env(java_home=fake_home):
            with mock.patch("os.path.isfile", return_value=True):
                with self._patch_run(stderr='openjdk version "17.0.1" 2021-10-19'):
                    info = detect_java()

        self.assertEqual(17, info.major_version)
        self.assertEqual(expected_exe, info.executable)

    def test_java_17_prerelease_ea(self):
        """'17-ea' pre-release string must resolve to major version 17."""
        with self._patch_env(), self._patch_which("/usr/bin/java"):
            with self._patch_run(stderr='openjdk version "17-ea" 2021-09-14'):
                info = detect_java()
        self.assertEqual(17, info.major_version)

    def test_java_21_returns_info(self):
        """Java 21 returns correct major version."""
        with self._patch_env(), self._patch_which("/usr/bin/java"):
            with self._patch_run(stderr='openjdk version "21.0.1" 2023-10-17'):
                info = detect_java()
        self.assertEqual(21, info.major_version)

    def test_bare_version_no_patch(self):
        """'11' with no minor/patch component still resolves correctly."""
        with self._patch_env(), self._patch_which("/usr/bin/java"):
            with self._patch_run(stderr='openjdk version "11"'):
                info = detect_java()
        self.assertEqual(11, info.major_version)

    def test_picked_up_line_is_ignored(self):
        """A 'Picked up JAVA_TOOL_OPTIONS' line before the version line must be ignored."""
        stderr = (
            "Picked up JAVA_TOOL_OPTIONS: -Xmx512m\n"
            'openjdk version "17.0.1" 2021-10-19\n'
            "OpenJDK Runtime Environment (build 17.0.1+12)\n"
        )
        with self._patch_env(), self._patch_which("/usr/bin/java"):
            with self._patch_run(stderr=stderr):
                info = detect_java()
        self.assertEqual(17, info.major_version)

    # --------------------------------------------------- not-found / bad-JAVA_HOME cases

    def test_no_java_anywhere_raises(self):
        """No JAVA_HOME and nothing on PATH must raise with a 'not found' message."""
        with self._patch_env(), self._patch_which(None):
            with self.assertRaises(JavaDetectionError) as ctx:
                detect_java()
        self.assertIn("Java not found", str(ctx.exception))
        self.assertIn("JAVA_HOME", str(ctx.exception))

    def test_java_home_set_but_binary_missing_raises(self):
        """JAVA_HOME set but binary absent must raise immediately, not fall back to PATH."""
        with self._patch_env(java_home="/bad/path"):
            with mock.patch("os.path.isfile", return_value=False):
                # shutil.which must NOT be consulted — if it is, the test would fail
                with mock.patch("seatunnel.java_detector.shutil.which", side_effect=AssertionError("should not reach PATH")):
                    with self.assertRaises(JavaDetectionError) as ctx:
                        detect_java()
        msg = str(ctx.exception)
        self.assertIn("/bad/path", msg)
        self.assertIn("JAVA_HOME is set to", msg)

    # ----------------------------------------------- subprocess failure cases

    def test_nonzero_exit_code_raises(self):
        """Non-zero return code from java -version must raise JavaDetectionError."""
        with self._patch_env(), self._patch_which("/usr/bin/java"):
            with self._patch_run(stderr="Error: JVM crashed", returncode=1):
                with self.assertRaises(JavaDetectionError) as ctx:
                    detect_java()
        self.assertIn("exited with code 1", str(ctx.exception))

    def test_unparseable_output_raises(self):
        """Output with no recognisable version string must raise with 'Could not determine'."""
        with self._patch_env(), self._patch_which("/usr/bin/java"):
            with self._patch_run(stderr="some garbled output"):
                with self.assertRaises(JavaDetectionError) as ctx:
                    detect_java()
        self.assertIn("Could not determine Java version", str(ctx.exception))

    def test_timeout_raises(self):
        """subprocess.TimeoutExpired must be caught and re-raised as JavaDetectionError."""
        with self._patch_env(), self._patch_which("/usr/bin/java"):
            with mock.patch(
                "seatunnel.java_detector.subprocess.run",
                side_effect=subprocess.TimeoutExpired(cmd=["java", "-version"], timeout=10),
            ):
                with self.assertRaises(JavaDetectionError) as ctx:
                    detect_java()
        self.assertIn("Timed out", str(ctx.exception))

    def test_oserror_raises(self):
        """OSError from subprocess.run must be caught and re-raised as JavaDetectionError."""
        with self._patch_env(), self._patch_which("/usr/bin/java"):
            with mock.patch(
                "seatunnel.java_detector.subprocess.run",
                side_effect=OSError("Permission denied"),
            ):
                with self.assertRaises(JavaDetectionError) as ctx:
                    detect_java()
        self.assertIn("Failed to run", str(ctx.exception))

    # ------------------------------------------------- custom min_version cases

    def test_custom_min_version_java8_accepted(self):
        """With min_version=8, Java 8 must succeed."""
        with self._patch_env(), self._patch_which("/usr/bin/java"):
            with self._patch_run(stderr='java version "1.8.0_292"'):
                info = detect_java(min_version=8)
        self.assertEqual(8, info.major_version)

    def test_error_message_uses_min_version_argument(self):
        """Too-old error must mention the *min_version* argument, not a hardcoded 11."""
        with self._patch_env(), self._patch_which("/usr/bin/java"):
            with self._patch_run(stderr='openjdk version "11.0.2"'):
                with self.assertRaises(JavaDetectionError) as ctx:
                    detect_java(min_version=17)
        self.assertIn("Java 17+", str(ctx.exception))
        self.assertIn("Java 11", str(ctx.exception))  # the found version

    def test_min_version_no_java_anywhere_error_contains_min_version(self):
        """With min_version=17 and no Java, error must say '17+' and must NOT say '11+'."""
        with self._patch_env(), self._patch_which(None):
            with self.assertRaises(JavaDetectionError) as ctx:
                detect_java(min_version=17)
        msg = str(ctx.exception)
        self.assertIn("17+", msg)
        self.assertNotIn("11+", msg)

    def test_min_version_bad_java_home_error_contains_min_version(self):
        """With min_version=17 and a bad JAVA_HOME, error must say '17+'."""
        with self._patch_env(java_home="/bad/path"):
            with mock.patch("os.path.isfile", return_value=False):
                with self.assertRaises(JavaDetectionError) as ctx:
                    detect_java(min_version=17)
        self.assertIn("17+", str(ctx.exception))

    def test_note_picked_up_jdk_java_options_line_is_ignored(self):
        """A 'NOTE: Picked up JDK_JAVA_OPTIONS' line with embedded quotes is skipped."""
        stderr = (
            'NOTE: Picked up JDK_JAVA_OPTIONS: -Dfoo="1"\n'
            'openjdk version "17.0.1" 2021-10-19\n'
            "OpenJDK Runtime Environment (build 17.0.1+12)\n"
        )
        with self._patch_env(), self._patch_which("/usr/bin/java"):
            with self._patch_run(stderr=stderr):
                info = detect_java()
        self.assertEqual(17, info.major_version)

    def test_version_in_stdout_only_is_parsed(self):
        """When stderr is empty but stdout contains the version line, it must be parsed."""
        with self._patch_env(), self._patch_which("/usr/bin/java"):
            with self._patch_run(stderr="", stdout='openjdk version "11.0.2" 2019-01-15'):
                info = detect_java()
        self.assertEqual(11, info.major_version)

    def test_java_home_too_old_error_mentions_java_home_value(self):
        """Java found via JAVA_HOME but too old: error must mention JAVA_HOME value, 'Java 8', and 'unset JAVA_HOME'."""
        fake_home = "/opt/jdk8"
        expected_binary = "java.exe" if sys.platform == "win32" else "java"
        expected_exe = os.path.join(fake_home, "bin", expected_binary)

        with self._patch_env(java_home=fake_home):
            with mock.patch("os.path.isfile", return_value=True):
                with self._patch_run(stderr='java version "1.8.0_292" 2021-04-20'):
                    with self.assertRaises(JavaDetectionError) as ctx:
                        detect_java()
        msg = str(ctx.exception)
        self.assertIn(fake_home, msg)
        self.assertIn("Java 8", msg)
        self.assertIn(expected_exe, msg)
        self.assertIn("unset JAVA_HOME", msg)


if __name__ == "__main__":
    unittest.main()

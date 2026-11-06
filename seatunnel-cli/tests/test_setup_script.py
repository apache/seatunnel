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

"""Behavioral tests for the source-install setup script without downloading packages."""

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


SETUP_SCRIPT = Path(__file__).resolve().parents[1] / "setup.sh"


class SetupScriptTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.cli_dir = self.root / "seatunnel-cli"
        self.cli_dir.mkdir()
        shutil.copy2(SETUP_SCRIPT, self.cli_dir / "setup.sh")
        self.bin_dir = self.root / "bin"
        self.bin_dir.mkdir()
        self.log = self.root / "calls.log"
        self._write_executable(
            "venv-python",
            """#!/usr/bin/env bash
echo "venv-python $*" >> "$SETUP_TEST_LOG"
exit 0
""",
        )
        self._write_executable(
            "python3",
            """#!/usr/bin/env bash
if [ "$1" = "-c" ]; then echo 3.11; exit 0; fi
echo "python3 $*" >> "$SETUP_TEST_LOG"
if [ "$1" = "-m" ] && [ "$2" = "venv" ]; then
    mkdir -p "$3/bin"
    if [ "$SETUP_TEST_MODE" = "fallback" ] || [ "$SETUP_TEST_MODE" = "fail" ]; then
        touch "$3/partial"
        exit 1
    fi
    cp "$SETUP_TEST_BIN/venv-python" "$3/bin/python"
    exit 0
fi
exit 1
""",
        )
        self._write_executable(
            "virtualenv",
            """#!/usr/bin/env bash
echo "virtualenv $*" >> "$SETUP_TEST_LOG"
dest="${@: -1}"
if [ -e "$dest/partial" ] || [ "$SETUP_TEST_MODE" = "fail" ]; then exit 1; fi
mkdir -p "$dest/bin"
cp "$SETUP_TEST_BIN/venv-python" "$dest/bin/python"
""",
        )

    def _write_executable(self, name, content):
        path = self.bin_dir / name
        path.write_text(content, encoding="utf-8")
        path.chmod(0o755)

    def _run_setup(self, mode="normal"):
        env = os.environ.copy()
        env.update({
            "PATH": str(self.bin_dir) + os.pathsep + env["PATH"],
            "SETUP_TEST_LOG": str(self.log),
            "SETUP_TEST_BIN": str(self.bin_dir),
            "SETUP_TEST_MODE": mode,
        })
        return subprocess.run(
            ["bash", str(self.cli_dir / "setup.sh")],
            cwd=self.root,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_creates_isolated_environment_and_reuses_it(self):
        first = self._run_setup()
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertTrue((self.cli_dir / ".venv/bin/python").exists())
        self.assertIn("source .venv/bin/activate", first.stdout)
        second = self._run_setup()
        self.assertEqual(second.returncode, 0, second.stderr)
        calls = self.log.read_text(encoding="utf-8")
        self.assertEqual(calls.count("python3 -m venv"), 1)
        self.assertEqual(calls.count("venv-python -m pip install --upgrade"), 2)
        self.assertEqual(calls.count("venv-python -m pip install -e .[dev]"), 2)
        self.assertNotIn("python3 -m pip", calls)

    def test_removes_partial_venv_before_virtualenv_fallback(self):
        result = self._run_setup("fallback")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue((self.cli_dir / ".venv/bin/python").exists())
        self.assertFalse((self.cli_dir / ".venv/partial").exists())
        self.assertIn("virtualenv --python=python3", self.log.read_text(encoding="utf-8"))

    def test_reports_missing_venv_support_without_global_install(self):
        old = self.cli_dir / ".venv"
        old.mkdir()
        (old / "important-marker").write_text("keep", encoding="utf-8")
        result = self._run_setup("fail")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("python3-venv (ensurepip) or virtualenv", result.stderr)
        self.assertEqual((old / "important-marker").read_text(encoding="utf-8"), "keep")
        self.assertNotIn("python3 -m pip", self.log.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()

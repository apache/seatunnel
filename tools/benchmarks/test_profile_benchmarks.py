#!/usr/bin/env python3
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

import os
import pathlib
import subprocess
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).with_name("profile_benchmarks.sh")


FAKE_JAVA = """#!/usr/bin/env bash
set -euo pipefail

for argument in "$@"; do
    if [[ "${argument}" == "-l" ]]; then
        printf 'Benchmarks:\n'
        case "${FAKE_LIST_MODE:-one}" in
            one)
                printf '%s\n' 'org.apache.seatunnel.benchmark.QueueBenchmark.publish'
                ;;
            two)
                printf '%s\n' \
                    'org.apache.seatunnel.benchmark.QueueBenchmark.publish' \
                    'org.apache.seatunnel.benchmark.QueueBenchmark.consume'
                ;;
            zero) ;;
        esac
        exit 0
    fi
done

printf '%s\n' "$@" > "${FAKE_JAVA_ARGS_FILE}"
result_file=''
profiler_directory=''
previous_argument=''
for argument in "$@"; do
    if [[ "${previous_argument}" == "-rff" ]]; then
        result_file="${argument}"
    fi
    case "${argument}" in
        async:*)
            profiler_directory="${argument##*;dir=}"
            profiler_directory="${profiler_directory%%;*}"
            ;;
    esac
    previous_argument="${argument}"
done
[[ -n "${result_file}" ]]
printf '[]\n' > "${result_file}"
if [[ -n "${profiler_directory}" ]]; then
    trial_directory="${profiler_directory}/fake-benchmark"
    mkdir -p "${trial_directory}"
    printf '%s\n' \
        '--- Execution profile ---' \
        "Total samples       : ${FAKE_ASYNC_SAMPLES:-42}" \
        > "${trial_directory}/summary-cpu.txt"
    printf 'jfr\n' > "${trial_directory}/jfr-cpu.jfr"
fi
"""


FAKE_JFRCONV = """#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$@" >> "${FAKE_CONVERTER_ARGS_FILE}"
for output_file in "$@"; do :; done
printf '<html>\nf(1)\n</html>\n' > "${output_file}"
"""


class ProfileBenchmarksTest(unittest.TestCase):

    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temporary_directory.name)
        self.repository = self.root / "candidate"
        target = self.repository / "seatunnel-benchmarks" / "target"
        target.mkdir(parents=True)
        (target / "benchmarks.jar").touch()

        fake_bin = self.root / "bin"
        fake_bin.mkdir()
        fake_java = fake_bin / "java"
        fake_java.write_text(FAKE_JAVA, encoding="utf-8")
        fake_java.chmod(0o755)

        profiler_home = self.root / "async-profiler"
        profiler_library = profiler_home / "lib" / "libasyncProfiler.dylib"
        profiler_library.parent.mkdir(parents=True)
        profiler_library.touch()
        profiler_converter = profiler_home / "bin" / "jfrconv"
        profiler_converter.parent.mkdir(parents=True)
        profiler_converter.write_text(FAKE_JFRCONV, encoding="utf-8")
        profiler_converter.chmod(0o755)

        self.java_arguments = self.root / "java-arguments.txt"
        self.converter_arguments = self.root / "converter-arguments.txt"
        self.environment = os.environ.copy()
        self.environment.pop("BENCHMARKS", None)
        self.environment.pop("PROFILE_JMH_ARGS", None)
        self.environment["FAKE_JAVA_ARGS_FILE"] = str(self.java_arguments)
        self.environment["FAKE_CONVERTER_ARGS_FILE"] = str(self.converter_arguments)
        self.environment["ASYNC_PROFILER_HOME"] = str(profiler_home)
        self.environment["PATH"] = "{}{}{}".format(
            fake_bin, os.pathsep, self.environment["PATH"]
        )

    def tearDown(self):
        self.temporary_directory.cleanup()

    def run_script(self, *arguments, environment=None):
        return subprocess.run(
            ["bash", str(SCRIPT), *arguments],
            cwd=self.root,
            env=environment or self.environment,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    def test_help_documents_repository_option(self):
        result = self.run_script("--help")

        self.assertEqual(0, result.returncode)
        self.assertIn("--repository DIR", result.stdout)

    def test_rejects_selector_that_matches_multiple_benchmarks(self):
        environment = self.environment.copy()
        environment["FAKE_LIST_MODE"] = "two"

        result = self.run_script(
            "profile",
            "gc",
            "--repository",
            str(self.repository),
            "--benchmark",
            ".*",
            environment=environment,
        )

        self.assertNotEqual(0, result.returncode)
        self.assertIn("matched 2", result.stderr)
        self.assertFalse(self.java_arguments.exists())

    def test_rejects_more_than_one_fork(self):
        result = self.run_script(
            "profile",
            "gc",
            "--repository",
            str(self.repository),
            "--",
            "-f",
            "2",
        )

        self.assertNotEqual(0, result.returncode)
        self.assertIn("require exactly one fork", result.stderr)
        self.assertFalse(self.java_arguments.exists())

    def test_runs_candidate_jar_with_one_fork(self):
        output = self.root / "diagnostics"

        result = self.run_script(
            "profile",
            "gc",
            "--repository",
            str(self.repository),
            "--output",
            str(output),
            "--",
            "-wi",
            "0",
            "-i",
            "1",
            "-r",
            "1s",
        )

        self.assertEqual(0, result.returncode, result.stderr)
        arguments = self.java_arguments.read_text(encoding="utf-8").splitlines()
        self.assertIn(str(self.repository / "seatunnel-benchmarks/target/benchmarks.jar"), arguments)
        self.assertEqual("1", arguments[arguments.index("-f") + 1])
        self.assertTrue((output / "jmh.log").is_file())
        self.assertTrue((output / "profile-report.json").is_file())
        self.assertTrue((output / "profile-summary.md").is_file())

    def test_rejects_nonempty_output_directory(self):
        output = self.root / "diagnostics"
        output.mkdir()
        (output / "old-profile.jfr").touch()

        result = self.run_script(
            "profile",
            "gc",
            "--repository",
            str(self.repository),
            "--output",
            str(output),
        )

        self.assertNotEqual(0, result.returncode)
        self.assertIn("Output directory is not empty", result.stderr)
        self.assertFalse(self.java_arguments.exists())

    def test_converts_async_jfr_to_forward_and_reverse_flame_graphs(self):
        output = self.root / "cpu-diagnostics"

        result = self.run_script(
            "profile",
            "cpu",
            "--repository",
            str(self.repository),
            "--output",
            str(output),
        )

        self.assertEqual(0, result.returncode, result.stderr)
        trial = output / "raw" / "fake-benchmark"
        self.assertIn("f(1)", (trial / "flame-cpu-forward.html").read_text())
        self.assertIn("f(1)", (trial / "flame-cpu-reverse.html").read_text())
        converter_arguments = self.converter_arguments.read_text(encoding="utf-8")
        self.assertIn("--cpu", converter_arguments)
        self.assertIn("--reverse", converter_arguments)

    def test_zero_lock_samples_do_not_create_empty_flame_graph(self):
        environment = self.environment.copy()
        environment["FAKE_ASYNC_SAMPLES"] = "0"
        output = self.root / "lock-diagnostics"

        result = self.run_script(
            "profile",
            "lock",
            "--repository",
            str(self.repository),
            "--output",
            str(output),
            environment=environment,
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertFalse(self.converter_arguments.exists())
        self.assertFalse(list(output.rglob("flame-*.html")))
        summary = (output / "profile-summary.md").read_text(encoding="utf-8")
        self.assertIn("Async-profiler samples: `0`", summary)
        self.assertIn("no flame graph was generated", summary)


if __name__ == "__main__":
    unittest.main()

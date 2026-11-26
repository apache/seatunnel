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
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from typing import Optional


class JavaDetectionError(Exception):
    """Raised when Java cannot be found, is too old, or its version cannot be determined."""


@dataclass(frozen=True)
class JavaInfo:
    """Holds the resolved Java executable path and its parsed major version."""

    executable: str
    major_version: int


# Matches the quoted version string in `java -version` output, e.g.:
#   java version "1.8.0_292"
#   openjdk version "11.0.2"
#   openjdk version "17-ea"
_VERSION_RE = re.compile(r'version "(\d+)(?:\.(\d+))?[^"]*"')

_TIMEOUT = 10
DEFAULT_MIN_JAVA_VERSION = 11


def _find_java_executable(min_version: int = DEFAULT_MIN_JAVA_VERSION) -> Optional[str]:
    """Return the path to the java binary, or None if not found.

    Resolution order:
      1. $JAVA_HOME/bin/java (raises JavaDetectionError if JAVA_HOME is set but invalid)
      2. First 'java' found on PATH (only when JAVA_HOME is unset or empty)
    """
    java_home = os.environ.get("JAVA_HOME", "").strip()
    if java_home:
        binary = "java.exe" if sys.platform == "win32" else "java"
        candidate = os.path.join(java_home, "bin", binary)
        if os.path.isfile(candidate):
            return candidate
        raise JavaDetectionError(
            f"JAVA_HOME is set to '{java_home}' but no java executable was found there. "
            f"Fix JAVA_HOME or unset it, or install JDK {min_version}+."
        )
    return shutil.which("java")


def _parse_major_version(output: str) -> Optional[int]:
    """Extract the Java major version from `java -version` output.

    Handles old-style "1.X.Y_ZZZ" (returns X) and new-style "X.Y.Z" / "X-ea" (returns X).
    Lines starting with "Picked up" are skipped (JVM tool options noise).
    Returns None if no version line is found.
    """
    for line in output.splitlines():
        if line.strip().startswith("Picked up"):
            continue
        match = _VERSION_RE.search(line)
        if match:
            first, second = match.group(1), match.group(2)
            if first == "1" and second is not None:
                # Old format: "1.8.0_292" → Java 8
                return int(second)
            return int(first)
    return None


def detect_java(min_version: int = DEFAULT_MIN_JAVA_VERSION) -> JavaInfo:
    """Locate a Java executable, verify it meets *min_version*, and return a :class:`JavaInfo`.

    Resolution order: ``$JAVA_HOME/bin/java`` first, then the first ``java`` on ``PATH``.

    :param min_version: Minimum required Java major version (default 11).
    :returns: :class:`JavaInfo` with the path and major version.
    :raises JavaDetectionError: If Java is not found, is below *min_version*, its version
        cannot be parsed, or the subprocess call fails.
    """
    executable = _find_java_executable(min_version)

    if executable is None:
        raise JavaDetectionError(
            f"Java not found. Set JAVA_HOME or install JDK {min_version}+."
        )

    # Run `java -version`; output goes to stderr by convention, but also check stdout.
    try:
        result = subprocess.run(
            [executable, "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=_TIMEOUT,
        )
    except subprocess.TimeoutExpired as exc:
        raise JavaDetectionError(
            f"Timed out running 'java -version' at '{executable}'. "
            f"Set JAVA_HOME or install JDK {min_version}+."
        ) from exc
    except OSError as exc:
        raise JavaDetectionError(
            f"Failed to run 'java -version' at '{executable}': {exc}. "
            f"Set JAVA_HOME or install JDK {min_version}+."
        ) from exc

    if result.returncode != 0:
        raw = result.stderr.decode(errors="replace").strip() or result.stdout.decode(errors="replace").strip()
        raise JavaDetectionError(
            f"'java -version' exited with code {result.returncode}: {raw!r}. "
            f"Set JAVA_HOME or install JDK {min_version}+."
        )

    stderr_text = result.stderr.decode(errors="replace")
    stdout_text = result.stdout.decode(errors="replace")

    # Try stderr first (standard), fall back to stdout.
    major = _parse_major_version(stderr_text) or _parse_major_version(stdout_text)

    if major is None:
        raw = (stderr_text + stdout_text).strip()
        raise JavaDetectionError(
            f"Could not determine Java version from output: {raw!r}. "
            f"Set JAVA_HOME or install JDK {min_version}+."
        )

    if major < min_version:
        java_home = os.environ.get("JAVA_HOME", "").strip()
        if java_home:
            raise JavaDetectionError(
                f"Java {major} found at '{executable}' (from JAVA_HOME='{java_home}'), "
                f"but Java {min_version}+ is required. "
                f"Fix or unset JAVA_HOME, or install JDK {min_version}+."
            )
        raise JavaDetectionError(
            f"Java {major} found at '{executable}', but Java {min_version}+ is required. "
            f"Set JAVA_HOME or install JDK {min_version}+."
        )

    return JavaInfo(executable=executable, major_version=major)

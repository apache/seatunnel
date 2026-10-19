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

"""Select connector plugins in ``config/plugin_config`` and run ``bin/install-plugin.sh``.

Ownership boundary: these helpers rewrite ``config/plugin_config`` in place. Only pass them
a SeaTunnel copy that the caller owns and manages, such as the launcher's per-user,
per-version cache directory. Never point them at a source checkout or at a shared
installation, because the connector selection there would be changed for everyone using it.
"""

import os
import re
import shutil
import subprocess
import tempfile
from typing import Iterable, List


class PluginInstallError(Exception):
    """Raised when connector plugins cannot be selected or installed."""


# Same character set that bin/install-plugin.sh accepts for a connector artifact ID.
_ARTIFACT_ID_RE = re.compile(r"[A-Za-z0-9._-]+")

# A connector line in config/plugin_config: enabled ("connector-jdbc") or disabled by a
# leading "#" ("#connector-jdbc"). Section headers such as "--connectors-v2--" start with
# "-" and license / description comments contain spaces, so neither matches.
_CONNECTOR_LINE_RE = re.compile(r"(#?)([A-Za-z0-9._][A-Za-z0-9._-]*)")

_CONNECTOR_PREFIX = "connector-"


def _normalize_connectors(connectors: Iterable[str]) -> List[str]:
    """Validate requested connector names and return artifact IDs in request order.

    Both short names ("jdbc") and artifact IDs ("connector-jdbc") are accepted.
    """
    if isinstance(connectors, str):
        connectors = [connectors]

    artifact_ids = []
    for name in connectors:
        name = name.strip() if isinstance(name, str) else ""
        if not _ARTIFACT_ID_RE.fullmatch(name) or name.startswith("-"):
            raise PluginInstallError(f"Invalid connector name {name!r}.")
        if not name.startswith(_CONNECTOR_PREFIX):
            name = _CONNECTOR_PREFIX + name
        if name not in artifact_ids:
            artifact_ids.append(name)

    if not artifact_ids:
        raise PluginInstallError("No connectors requested.")
    return artifact_ids


def select_plugins(plugin_config_path: str, connectors: Iterable[str]) -> List[str]:
    """Rewrite *plugin_config_path* so that only *connectors* are enabled.

    Connector lines that were not requested are disabled with a leading ``#`` instead of
    being removed, so a later call can enable them again. Section headers, comments,
    blank lines and line endings are kept as they are. Running it again with the same
    connectors leaves the file unchanged.

    :param plugin_config_path: Path to ``config/plugin_config`` in a caller-managed copy
        (see the module docstring); never a source checkout or a shared installation.
    :param connectors: Short names (``"jdbc"``) or artifact IDs (``"connector-jdbc"``).
    :returns: The enabled artifact IDs, in request order.
    :raises PluginInstallError: If a name is invalid, a connector is not listed in the
        file, or the file cannot be read or written. The file is not modified then.
    """
    artifact_ids = _normalize_connectors(connectors)

    try:
        with open(plugin_config_path, encoding="utf-8", newline="") as f:
            original = f.read()
    except OSError as exc:
        raise PluginInstallError(
            f"Cannot read plugin config '{plugin_config_path}': {exc}"
        ) from exc

    wanted = set(artifact_ids)
    found = set()
    lines = []
    for line in original.splitlines(keepends=True):
        content = line.rstrip("\r\n")
        match = _CONNECTOR_LINE_RE.fullmatch(content)
        if match:
            artifact_id = match.group(2)
            if artifact_id in wanted:
                found.add(artifact_id)
                content = artifact_id
            else:
                content = "#" + artifact_id
            line = content + line[len(line.rstrip("\r\n")):]
        lines.append(line)

    missing = [a for a in artifact_ids if a not in found]
    if missing:
        raise PluginInstallError(
            f"Connector(s) not found in '{plugin_config_path}': {', '.join(missing)}."
        )

    updated = "".join(lines)
    if updated != original:
        _write_atomically(plugin_config_path, updated.encode("utf-8"))
    return artifact_ids


def _write_atomically(path: str, content: bytes) -> None:
    """Write *content* to *path* through a temporary file, so a failure never leaves it half written."""
    directory = os.path.dirname(os.path.abspath(path))
    try:
        fd, temp_path = tempfile.mkstemp(dir=directory, prefix=".plugin_config.")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(content)
            shutil.copymode(path, temp_path)
            os.replace(temp_path, path)
        except BaseException:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise
    except OSError as exc:
        raise PluginInstallError(f"Cannot write plugin config '{path}': {exc}") from exc


def install_plugins(seatunnel_home: str, connectors: Iterable[str], version: str) -> List[str]:
    """Enable *connectors* in ``config/plugin_config`` and run ``bin/install-plugin.sh <version>``.

    The script's output is not captured, so download progress is shown to the user.
    Environment variables such as ``SEATUNNEL_PLUGIN_DOWNLOAD_METHOD`` and
    ``SEATUNNEL_MAVEN_REPOSITORY`` are passed through to it.

    If the script fails, cannot be started or is interrupted, ``plugin_config`` is restored
    to its exact content before the call, so a failed install leaves no changed selection.

    :param seatunnel_home: Root of a caller-managed SeaTunnel distribution, such as the
        launcher's per-user, per-version cache (see the module docstring); never a source
        checkout or a shared installation.
    :param connectors: Short names (``"jdbc"``) or artifact IDs (``"connector-jdbc"``).
    :param version: Connector version to install; it should match the engine version.
    :returns: The installed artifact IDs, in request order.
    :raises PluginInstallError: If the distribution layout is incomplete, ``bash`` is not
        available, a connector is not listed, or the script fails.
    """
    version = version.strip() if isinstance(version, str) else ""
    if not version:
        raise PluginInstallError("A connector version is required, e.g. '3.0.1'.")

    plugin_config = os.path.join(seatunnel_home, "config", "plugin_config")
    script = os.path.join(seatunnel_home, "bin", "install-plugin.sh")
    for path in (plugin_config, script):
        if not os.path.isfile(path):
            raise PluginInstallError(
                f"'{path}' not found. Is '{seatunnel_home}' a SeaTunnel distribution?"
            )

    # Check everything that can fail before the script starts, so a doomed install does
    # not leave plugin_config rewritten.
    bash = shutil.which("bash")
    if bash is None:
        raise PluginInstallError("bash is required to run 'bin/install-plugin.sh'.")

    try:
        with open(plugin_config, "rb") as f:
            original = f.read()
    except OSError as exc:
        raise PluginInstallError(
            f"Cannot read plugin config '{plugin_config}': {exc}"
        ) from exc

    artifact_ids = select_plugins(plugin_config, connectors)

    try:
        try:
            result = subprocess.run([bash, script, version], cwd=seatunnel_home, check=False)
        except OSError as exc:
            raise PluginInstallError(f"Failed to run '{script}': {exc}") from exc

        if result.returncode != 0:
            raise PluginInstallError(
                f"'{script} {version}' exited with code {result.returncode} while installing "
                f"{', '.join(artifact_ids)}. See the output above for details."
            )
    except BaseException as exc:
        # Also covers KeyboardInterrupt during a long download.
        try:
            _write_atomically(plugin_config, original)
        except PluginInstallError as restore_error:
            message = f"Restoring the original plugin config also failed: {restore_error}"
            if not isinstance(exc, Exception):
                # Keep KeyboardInterrupt / SystemExit as they are, so the caller still stops.
                # Raising it here records the restore failure as its __context__.
                if hasattr(exc, "add_note"):
                    exc.add_note(message)
                raise exc
            raise PluginInstallError(f"{exc} {message}") from exc
        raise
    return artifact_ids

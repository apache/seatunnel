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

:func:`install_plugins` runs ``bin/install-plugin.sh`` with bash and is supported on Linux
and macOS only. It holds an exclusive lock on ``config/.plugin_config.lock`` for the whole
select / install / restore sequence, so concurrent calls on the same copy run one after
another. :func:`select_plugins` works on any platform but takes no lock itself.
"""

import contextlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
from typing import Iterable, Iterator, List


class PluginInstallError(Exception):
    """Raised when connector plugins cannot be selected or installed."""


# Same character set that bin/install-plugin.sh accepts for a connector artifact ID and for a
# release version.
_ARTIFACT_ID_RE = re.compile(r"[A-Za-z0-9._-]+")

# A connector artifact ID. It cannot start with "-", which install-plugin.sh treats as a
# section header such as "--connectors-v2--".
_CONNECTOR_ID_RE = re.compile(r"[A-Za-z0-9._][A-Za-z0-9._-]*")

# A connector disabled by a leading "#" ("#connector-jdbc"). License and description comments
# contain spaces, so they do not match.
_DISABLED_CONNECTOR_RE = re.compile(r"#(" + _CONNECTOR_ID_RE.pattern + r")")

_CONNECTOR_PREFIX = "connector-"

_LOCK_FILE_NAME = ".plugin_config.lock"


def _normalize_connectors(connectors: Iterable[str]) -> List[str]:
    """Validate requested connector names and return artifact IDs in request order.

    Both short names ("jdbc") and artifact IDs ("connector-jdbc") are accepted.
    """
    if isinstance(connectors, str):
        connectors = [connectors]
    try:
        names = list(connectors)
    except TypeError as exc:
        raise PluginInstallError(
            f"Connectors must be a name or an iterable of names, got {connectors!r}."
        ) from exc

    artifact_ids = []
    for raw in names:
        if not isinstance(raw, str):
            raise PluginInstallError(f"Invalid connector name {raw!r}: expected a string.")
        name = raw.strip()
        if not _CONNECTOR_ID_RE.fullmatch(name):
            raise PluginInstallError(f"Invalid connector name {raw!r}.")
        if not name.startswith(_CONNECTOR_PREFIX):
            name = _CONNECTOR_PREFIX + name
        if name not in artifact_ids:
            artifact_ids.append(name)

    if not artifact_ids:
        raise PluginInstallError("No connectors requested.")
    return artifact_ids


def select_plugins(plugin_config_path: str, connectors: Iterable[str]) -> List[str]:
    """Rewrite *plugin_config_path* so that only *connectors* are enabled.

    Lines are classified the same way ``bin/install-plugin.sh`` reads them: blank lines and
    lines starting with ``-`` (section headers) or ``#`` (comments) are skipped, and every
    other line is a connector. Connector lines that were not requested are disabled with a
    leading ``#`` instead of being removed, so a later call can enable them again. Stray
    whitespace around a connector ID is dropped when its line is rewritten. Section headers,
    comments, blank lines and line endings are kept as they are. Running it again with the
    same connectors leaves the file unchanged.

    CRLF line endings are preserved, but ``bin/install-plugin.sh`` itself only accepts LF
    endings: it rejects a connector line ending in ``\\r`` as an invalid artifact ID. Give
    :func:`install_plugins` a ``plugin_config`` with LF endings, as shipped in the
    distribution.

    :param plugin_config_path: Path to ``config/plugin_config`` in a caller-managed copy
        (see the module docstring); never a source checkout or a shared installation.
    :param connectors: Short names (``"jdbc"``) or artifact IDs (``"connector-jdbc"``).
    :returns: The enabled artifact IDs, in request order.
    :raises PluginInstallError: If a name is invalid, a connector is not listed in the
        file, the file contains a line that ``install-plugin.sh`` would reject, or the file
        cannot be read, decoded or written. The file is not modified then.
    """
    artifact_ids = _normalize_connectors(connectors)

    try:
        with open(plugin_config_path, encoding="utf-8", newline="") as f:
            original = f.read()
    except (OSError, UnicodeDecodeError) as exc:
        raise PluginInstallError(
            f"Cannot read plugin config '{plugin_config_path}': {exc}"
        ) from exc

    wanted = set(artifact_ids)
    found = set()
    lines = []
    for number, line in enumerate(original.splitlines(keepends=True), start=1):
        content = line.rstrip("\r\n")
        ending = line[len(content):]
        first_char = content[:1]

        if first_char in ("", "-"):
            # Blank line or section header: install-plugin.sh skips it.
            lines.append(line)
            continue
        if first_char == "#":
            match = _DISABLED_CONNECTOR_RE.fullmatch(content.rstrip())
            if match is None:
                # An ordinary comment.
                lines.append(line)
                continue
            artifact_id = match.group(1)
        else:
            # install-plugin.sh downloads this line as a connector and exits if it is not a
            # valid artifact ID, so reject such a line before anything is rewritten.
            artifact_id = content.strip()
            if not _CONNECTOR_ID_RE.fullmatch(artifact_id):
                raise PluginInstallError(
                    f"Line {number} of '{plugin_config_path}' is not a valid connector "
                    f"artifact ID: {content!r}. bin/install-plugin.sh would reject it; "
                    f"fix the line or comment it out with '#'."
                )

        if artifact_id in wanted:
            found.add(artifact_id)
            lines.append(artifact_id + ending)
        else:
            lines.append("#" + artifact_id + ending)

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


def _restore_if_changed(path: str, original: bytes) -> None:
    """Write *original* back to *path*, unless the file still holds exactly those bytes."""
    try:
        with open(path, "rb") as f:
            current = f.read()
    except OSError:
        current = None
    if current != original:
        _write_atomically(path, original)


def _is_windows() -> bool:
    return sys.platform == "win32"


@contextlib.contextmanager
def _exclusive_lock(config_dir: str) -> Iterator[None]:
    """Hold an exclusive lock on ``<config_dir>/.plugin_config.lock`` while the block runs.

    The lock file is left in place: deleting it would let a waiting process lock an inode
    that a new caller no longer sees. Closing the descriptor releases the lock.
    """
    import fcntl  # POSIX only; install_plugins rejects Windows before getting here.

    lock_path = os.path.join(config_dir, _LOCK_FILE_NAME)
    try:
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
    except OSError as exc:
        raise PluginInstallError(f"Cannot open lock file '{lock_path}': {exc}") from exc
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
        except OSError as exc:
            raise PluginInstallError(f"Cannot lock '{lock_path}': {exc}") from exc
        yield
    finally:
        os.close(fd)


def install_plugins(seatunnel_home: str, connectors: Iterable[str], version: str) -> List[str]:
    """Enable *connectors* in ``config/plugin_config`` and run ``bin/install-plugin.sh <version>``.

    Supported on Linux and macOS only. ``bin/install-plugin.cmd`` does not report download
    failures through its exit code, so it cannot be used with the rollback below, and a
    WSL or Git ``bash`` cannot run the script from a Windows path.

    The script's output is not captured, so download progress is shown to the user.
    Environment variables such as ``SEATUNNEL_PLUGIN_DOWNLOAD_METHOD`` and
    ``SEATUNNEL_MAVEN_REPOSITORY`` are passed through to it.

    If the script fails, cannot be started or is interrupted, ``plugin_config`` is restored
    to its exact content before the call, so a failed install leaves no changed selection.
    The whole sequence runs under an exclusive lock on ``config/.plugin_config.lock``, so a
    concurrent call on the same copy waits instead of installing another call's selection.

    :param seatunnel_home: Root of a caller-managed SeaTunnel distribution, such as the
        launcher's per-user, per-version cache (see the module docstring); never a source
        checkout or a shared installation. A relative path is resolved against the current
        working directory.
    :param connectors: Short names (``"jdbc"``) or artifact IDs (``"connector-jdbc"``).
    :param version: Connector version to install; it should match the SeaTunnel engine
        version of the distribution. It may only contain ``A-Z a-z 0-9 . _ -``, the same
        characters ``install-plugin.sh`` accepts. ``LATEST``, ``RELEASE`` and ``*-SNAPSHOT``
        make the script download through Maven (``mvnw``) instead of over HTTPS.
    :returns: The installed artifact IDs, in request order.
    :raises PluginInstallError: If the platform is Windows, the version is invalid, the
        distribution layout is incomplete, ``bash`` is not available, a connector is not
        listed, ``plugin_config`` cannot be parsed, or the script fails.
    """
    # Check everything that can fail before the script starts, so a doomed install does
    # not leave plugin_config rewritten.
    if _is_windows():
        raise PluginInstallError(
            "install_plugins() is supported on Linux and macOS only: it runs "
            "bin/install-plugin.sh with bash. select_plugins() works on any platform."
        )

    version = version.strip() if isinstance(version, str) else ""
    if not version:
        raise PluginInstallError(
            "A connector version is required; it should match your SeaTunnel engine version."
        )
    if not _ARTIFACT_ID_RE.fullmatch(version):
        raise PluginInstallError(
            f"Invalid connector version {version!r}: only letters, digits, '.', '_' and '-' "
            f"are allowed. It should match your SeaTunnel engine version."
        )

    if not isinstance(seatunnel_home, (str, os.PathLike)) or not isinstance(
        os.fspath(seatunnel_home), str
    ):
        raise PluginInstallError(
            f"seatunnel_home must be a path to a SeaTunnel distribution, got {seatunnel_home!r}."
        )
    # The script path is passed to bash while cwd is seatunnel_home, so it must be absolute.
    seatunnel_home = os.path.abspath(os.path.expanduser(os.fspath(seatunnel_home)))
    config_dir = os.path.join(seatunnel_home, "config")
    plugin_config = os.path.join(config_dir, "plugin_config")
    script = os.path.join(seatunnel_home, "bin", "install-plugin.sh")
    for path in (plugin_config, script):
        if not os.path.isfile(path):
            raise PluginInstallError(
                f"'{path}' not found. Is '{seatunnel_home}' a SeaTunnel distribution?"
            )

    bash = shutil.which("bash")
    if bash is None:
        raise PluginInstallError("bash is required to run 'bin/install-plugin.sh'.")

    with _exclusive_lock(config_dir):
        try:
            with open(plugin_config, "rb") as f:
                original = f.read()
        except OSError as exc:
            raise PluginInstallError(
                f"Cannot read plugin config '{plugin_config}': {exc}"
            ) from exc

        try:
            # Inside the try, so an interrupt right after the rewrite is rolled back too.
            artifact_ids = select_plugins(plugin_config, connectors)
            try:
                result = subprocess.run(
                    [bash, script, version], cwd=seatunnel_home, check=False
                )
            except OSError as exc:
                raise PluginInstallError(f"Failed to run '{script}': {exc}") from exc

            if result.returncode != 0:
                raise PluginInstallError(
                    f"'{script} {version}' exited with code {result.returncode} while "
                    f"installing {', '.join(artifact_ids)}. See the output above for details."
                )
        except BaseException as exc:
            # Also covers KeyboardInterrupt during a long download.
            try:
                _restore_if_changed(plugin_config, original)
            except PluginInstallError as restore_error:
                message = f"Restoring the original plugin config also failed: {restore_error}"
                if not isinstance(exc, Exception):
                    # Keep KeyboardInterrupt / SystemExit as they are, so the caller still
                    # stops. Raising it here records the restore failure as its __context__.
                    if hasattr(exc, "add_note"):
                        exc.add_note(message)
                    raise exc
                raise PluginInstallError(f"{exc} {message}") from exc
            raise
    return artifact_ids

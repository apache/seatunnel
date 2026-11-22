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

"""Locate the per-user cache of a SeaTunnel distribution and build its download URL.

Ownership boundary: :func:`cache_dir` is the per-user, per-version directory that the
launcher owns. Later launcher steps (download, extraction, plugin installation) may modify
this directory and nothing else. It is never inside a source checkout, a shared
installation or the Python package directory (``site-packages``), so installing or
upgrading the Python package neither touches nor loses a downloaded distribution.

This module only computes locations. It creates no directory, opens no connection and
downloads nothing; those belong to later steps of
https://github.com/apache/seatunnel/issues/12707.
"""

import os
import re
import sys
from urllib.parse import urlsplit

DEFAULT_BASE_URL = "https://downloads.apache.org/seatunnel"

_CACHE_DIR_NAME = "seatunnel"

# A release version is used as a URL path segment and as a directory name, so it must start
# and end with a letter or digit. Starting with one rules out "." and "..", which would let a
# version name a directory outside the cache. Ending with one keeps "3.0." and "3.0" apart on
# Windows, which drops a trailing dot from directory names.
_VERSION_RE = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?")


class DistributionError(Exception):
    """Raised when a distribution location cannot be determined."""


def _validate_version(version: str) -> str:
    if not isinstance(version, str) or not _VERSION_RE.fullmatch(version.strip()):
        raise DistributionError(
            f"Invalid SeaTunnel version {version!r}: it must start and end with a letter or "
            f"digit and may only contain letters, digits, '.', '_' and '-'."
        )
    return version.strip()


def distribution_url(version: str, base_url: str = DEFAULT_BASE_URL) -> str:
    """Return the URL of the binary distribution archive of a SeaTunnel release.

    The result is deterministic and needs no network access. For ``3.0.0`` it is
    ``https://downloads.apache.org/seatunnel/3.0.0/apache-seatunnel-3.0.0-bin.tar.gz``. The
    checksum and signature published next to the archive are at the same URL with a
    ``.sha512`` or ``.asc`` suffix.

    Note that downloads.apache.org only keeps current releases; the ASF moves older ones to
    archive.apache.org, which can be passed as *base_url* by the caller.

    :param version: Release version such as ``"3.0.0"``. It may only contain
        ``A-Z a-z 0-9 . _ -`` and must start and end with a letter or digit.
    :param base_url: Root of the release repository, such as
        ``https://archive.apache.org/dist/seatunnel``. It must be an ``https://`` URL with a
        host and no query or fragment, so the archive, checksum and signature cannot be
        altered in transit.
    :raises DistributionError: If the version or base URL is invalid.
    """
    version = _validate_version(version)
    parts = urlsplit(base_url) if isinstance(base_url, str) else None
    if (
        parts is None
        or parts.scheme != "https"
        or not parts.netloc
        or parts.query
        or parts.fragment
    ):
        raise DistributionError(
            f"The distribution base URL must be an 'https://' URL with a host and no query "
            f"or fragment, got {base_url!r}."
        )
    return f"{base_url.rstrip('/')}/{version}/apache-seatunnel-{version}-bin.tar.gz"


def _cache_root() -> str:
    """Return the per-user cache root of the current platform."""
    home = os.path.expanduser("~")
    if sys.platform == "win32":
        base = os.environ.get("LOCALAPPDATA") or os.path.join(home, "AppData", "Local")
    elif sys.platform == "darwin":
        base = os.path.join(home, "Library", "Caches")
    else:
        # The XDG specification says to ignore a relative XDG_CACHE_HOME.
        base = os.environ.get("XDG_CACHE_HOME", "")
        if not os.path.isabs(base):
            base = os.path.join(home, ".cache")
    if not os.path.isabs(base):
        raise DistributionError(
            f"Cannot determine a per-user cache location: '{base}' is not an absolute path."
        )
    return os.path.join(base, _CACHE_DIR_NAME)


def cache_dir(version: str) -> str:
    """Return the per-user directory that holds the distribution of *version*.

    Each user and each version gets a separate directory, so two versions or two users never
    share or overwrite files. The location is ``seatunnel/<version>`` under the platform's
    per-user cache directory: ``$XDG_CACHE_HOME`` or ``~/.cache`` on Linux,
    ``~/Library/Caches`` on macOS and ``%LOCALAPPDATA%`` on Windows. The directory is not
    created and may not exist.

    :param version: Release version such as ``"3.0.0"``; see :func:`distribution_url`.
    :returns: An absolute path.
    :raises DistributionError: If the version is invalid or no absolute per-user cache
        location can be determined.
    """
    return os.path.join(_cache_root(), _validate_version(version))

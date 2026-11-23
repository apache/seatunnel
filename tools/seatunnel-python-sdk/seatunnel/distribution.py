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
this directory and nothing else. By default it is under the user's own cache directory,
outside any source checkout, shared installation or the Python package directory
(``site-packages``), so installing or upgrading the Python package neither touches nor loses
a downloaded distribution. The location follows the user's home directory and
``XDG_CACHE_HOME`` (Linux) or ``LOCALAPPDATA`` (Windows); a relative value or one that
resolves to the filesystem root is not used, but the location is not otherwise checked, so
pointing those at a shared directory makes the cache shared too.

This module only computes locations. It creates no directory, opens no connection and
downloads nothing; those belong to later steps of
https://github.com/apache/seatunnel/issues/12707.
"""

import os
import re
import sys
from typing import Optional
from urllib.parse import urlsplit

DEFAULT_BASE_URL = "https://downloads.apache.org/seatunnel"

_CACHE_DIR_NAME = "seatunnel"

# A release version is used as a URL path segment and as a directory name. It must start with
# a digit, as every SeaTunnel release does: that rules out "." and "..", which would let a
# version name a directory outside the cache, and the Windows device names such as "nul",
# "con" or "com1", which are not directories. It must end with a letter or digit, because
# Windows drops a trailing dot from directory names and "3.0." would share "3.0"'s directory.
_VERSION_RE = re.compile(r"[0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?")

# Characters allowed in a base URL: the RFC 3986 unreserved and reserved characters and "%",
# without "?" and "#". That excludes spaces, control characters, non-ASCII characters and "\",
# and keeps a query or fragment from swallowing the version and file name appended to it.
_BASE_URL_RE = re.compile(r"[A-Za-z0-9._~:/@!$&'()*+,;=%\[\]-]+")

# A "%" that does not start a two-digit hexadecimal escape such as "%20".
_BAD_PERCENT_RE = re.compile(r"%(?![0-9A-Fa-f]{2})")

# The user information ("user:password@") of a URL, hidden in error messages.
_USERINFO_RE = re.compile(r"(//)[^/]*@")


class DistributionError(Exception):
    """Raised when a distribution location cannot be determined."""


def _validate_version(version: str) -> str:
    if not isinstance(version, str) or not _VERSION_RE.fullmatch(version.strip()):
        raise DistributionError(
            f"Invalid SeaTunnel version {version!r}: it must start with a digit, end with a "
            f"letter or digit, and may only contain letters, digits, '.', '_' and '-'."
        )
    return version.strip()


def _validate_base_url(base_url: str) -> str:
    """Return *base_url* without surrounding whitespace and trailing slashes, if it is valid."""
    if not isinstance(base_url, str):
        raise DistributionError(f"The distribution base URL must be a string, got {base_url!r}.")
    url = base_url.strip()
    shown = _USERINFO_RE.sub(r"\1***@", url)
    if not _BASE_URL_RE.fullmatch(url):
        problem = (
            "it may only contain characters allowed in a URL path, without spaces, control "
            "characters, '\\', '?' or '#'"
        )
    elif _BAD_PERCENT_RE.search(url):
        problem = "'%' must start a two-digit hexadecimal escape such as '%20'"
    else:
        try:
            parts = urlsplit(url)
            hostname, port = parts.hostname, parts.port
        except ValueError as exc:
            raise DistributionError(f"Invalid distribution base URL {shown!r}: {exc}.") from exc
        if parts.scheme != "https":
            problem = "it must start with 'https://'"
        elif not hostname:
            problem = "it has no host"
        elif parts.username is not None or parts.password is not None:
            problem = "it must not contain credentials"
        elif "[" in parts.path or "]" in parts.path:
            problem = "'[' and ']' are only allowed around an IPv6 host"
        elif port == 0:
            problem = "port 0 is not a valid port"
        else:
            return url.rstrip("/")
    raise DistributionError(f"Invalid distribution base URL {shown!r}: {problem}.")


def distribution_url(version: str, base_url: str = DEFAULT_BASE_URL) -> str:
    """Return the URL of the binary distribution archive of a SeaTunnel release.

    The result is deterministic and needs no network access. For ``3.0.0`` it is
    ``https://downloads.apache.org/seatunnel/3.0.0/apache-seatunnel-3.0.0-bin.tar.gz``. The
    checksum and signature published next to the archive are at the same URL with a
    ``.sha512`` or ``.asc`` suffix.

    Note that downloads.apache.org only keeps current releases; the ASF moves older ones to
    archive.apache.org, which can be passed as *base_url* by the caller.

    :param version: Release version such as ``"3.0.0"``. It must start with a digit, end with
        a letter or digit, and may only contain ``A-Z a-z 0-9 . _ -``. Surrounding whitespace
        is dropped.
    :param base_url: Root of the release repository, such as
        ``https://archive.apache.org/dist/seatunnel``. It must be an ``https://`` URL with a
        host, so the archive, checksum and signature cannot be altered in transit. It must not
        contain credentials, a query (``?``), a fragment (``#``), or characters that are not
        valid in a URL path, such as spaces, control characters, non-ASCII characters, ``\\``,
        a malformed ``%`` escape or ``[`` / ``]`` outside an IPv6 host. Surrounding
        whitespace and trailing slashes are dropped.
    :raises DistributionError: If the version or base URL is invalid.
    """
    version = _validate_version(version)
    url = _validate_base_url(base_url)
    return f"{url}/{version}/apache-seatunnel-{version}-bin.tar.gz"


def _usable_dir(path: Optional[str]) -> Optional[str]:
    """Return *path* if it is an absolute path other than the filesystem root, else ``None``.

    The filesystem root is refused because every user whose home or cache variable resolves to
    it (for example ``HOME=/`` in a container) would share the same cache.
    """
    if not path or not os.path.isabs(path):
        return None
    normalized = os.path.normpath(path)
    if os.path.dirname(normalized) == normalized:
        return None
    return path


def _cache_root() -> str:
    """Return the per-user cache root of the current platform."""
    if sys.platform == "win32":
        base = _usable_dir(os.environ.get("LOCALAPPDATA"))
        fallback = ("AppData", "Local")
        variables = "LOCALAPPDATA or USERPROFILE"
    elif sys.platform == "darwin":
        base = None
        fallback = ("Library", "Caches")
        variables = "HOME"
    else:
        # The XDG specification says to ignore a relative XDG_CACHE_HOME.
        base = _usable_dir(os.environ.get("XDG_CACHE_HOME"))
        fallback = (".cache",)
        variables = "XDG_CACHE_HOME or HOME"

    if base is None:
        # expanduser returns "~" unchanged when the home directory is unknown, and "/" when
        # HOME is empty or "/".
        home = os.path.expanduser("~")
        if _usable_dir(home) is None:
            raise DistributionError(
                f"Cannot determine a per-user cache location: the home directory resolves to "
                f"{home!r}, which is not an absolute path or is the filesystem root. Set "
                f"{variables} to a directory owned by the current user."
            )
        base = os.path.join(home, *fallback)
    return os.path.join(base, _CACHE_DIR_NAME)


def cache_dir(version: str) -> str:
    """Return the per-user directory that holds the distribution of *version*.

    The location is ``seatunnel/<version>`` under the platform's per-user cache directory:
    ``$XDG_CACHE_HOME`` or ``~/.cache`` on Linux, ``~/Library/Caches`` on macOS and
    ``%LOCALAPPDATA%`` (or ``~\\AppData\\Local``) on Windows. A relative value or the
    filesystem root in one of those variables is ignored. By default each user and each
    version gets a separate directory; see the module docstring for what is not checked.
    The directory is not created and may not exist.

    Versions are case-sensitive in the download URL, but Windows and macOS file systems are
    usually not, so ``3.0.0-SNAPSHOT`` and ``3.0.0-snapshot`` share one directory there.

    :param version: Release version such as ``"3.0.0"``; see :func:`distribution_url`.
    :returns: An absolute path.
    :raises DistributionError: If the version is invalid or no per-user cache location can be
        determined; the message names the variable to set.
    """
    return os.path.join(_cache_root(), _validate_version(version))

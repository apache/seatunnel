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


import contextlib
import os
import tempfile
import unittest
from unittest import mock

from seatunnel import distribution
from seatunnel.distribution import DistributionError, cache_dir, distribution_url

ARCHIVE_2_3_9 = "https://archive.apache.org/dist/seatunnel/2.3.9/apache-seatunnel-2.3.9-bin.tar.gz"


class TestDistributionUrl(unittest.TestCase):
    def test_release_url_follows_the_apache_layout(self):
        self.assertEqual(
            "https://downloads.apache.org/seatunnel/3.0.0/apache-seatunnel-3.0.0-bin.tar.gz",
            distribution_url("3.0.0"),
        )

    def test_url_is_deterministic(self):
        self.assertEqual(distribution_url("3.0.0"), distribution_url("3.0.0"))

    def test_each_version_gets_its_own_url(self):
        self.assertNotEqual(distribution_url("3.0.0"), distribution_url("3.0.1"))

    def test_surrounding_whitespace_in_version_is_dropped(self):
        self.assertEqual(distribution_url("3.0.0"), distribution_url(" 3.0.0\n"))

    def test_valid_versions_are_accepted(self):
        for version in ("3", "2.3.9", "3.0.0", "3.0.0-SNAPSHOT", "3.0.0-RC1", "3.0.0_beta"):
            with self.subTest(version=version):
                self.assertTrue(
                    distribution_url(version).endswith(
                        f"/{version}/apache-seatunnel-{version}-bin.tar.gz"
                    )
                )

    def test_invalid_versions_raise(self):
        for version in ("", "  ", ".", "..", "../x", "-3.0.0", "3.0.", "3.0-", "3.0/1",
                        "3.0\\1", "3.0 beta", "3.0;rm", "3.0?x=1", "3.0#frag", "v3.0.0",
                        "nul", "con", "aux", "prn", "com1", "lpt1", None, 3):
            with self.subTest(version=version):
                with self.assertRaises(DistributionError):
                    distribution_url(version)

    def test_custom_base_url_is_used_and_trailing_slash_is_ignored(self):
        for base_url in ("https://archive.apache.org/dist/seatunnel",
                         "https://archive.apache.org/dist/seatunnel/",
                         "https://archive.apache.org/dist/seatunnel//"):
            with self.subTest(base_url=base_url):
                self.assertEqual(ARCHIVE_2_3_9, distribution_url("2.3.9", base_url))

    def test_surrounding_whitespace_in_base_url_is_dropped(self):
        for base_url in ("https://archive.apache.org/dist/seatunnel/ ",
                         "https://archive.apache.org/dist/seatunnel\n",
                         " https://archive.apache.org/dist/seatunnel\t"):
            with self.subTest(base_url=base_url):
                self.assertEqual(ARCHIVE_2_3_9, distribution_url("2.3.9", base_url))

    def test_valid_hosts_and_ports_are_accepted(self):
        for base_url in ("https://mirror.example.org:8443/seatunnel",
                         "https://127.0.0.1/seatunnel", "https://[::1]/seatunnel",
                         "https://mirror.example.org", "https://x/apache%20seatunnel"):
            with self.subTest(base_url=base_url):
                self.assertEqual(
                    f"{base_url}/3.0.0/apache-seatunnel-3.0.0-bin.tar.gz",
                    distribution_url("3.0.0", base_url),
                )

    def test_invalid_base_url_raises(self):
        for base_url in (
            # Not HTTPS, or not a URL.
            "http://downloads.apache.org/seatunnel", "ftp://x/y", "downloads.apache.org/x", "",
            "  ",
            # No host, or an authority urlsplit cannot parse.
            "https://", "https:///seatunnel", "https://:443/seatunnel", "https://user@/seatunnel",
            "https://[::1/seatunnel", "https://host:abc/seatunnel", "https://host:99999/seatunnel",
            "https://host:0/seatunnel",
            # Credentials.
            "https://user@host/seatunnel", "https://user:secret@host/seatunnel",
            # A query or fragment, also with nothing after the delimiter.
            "https://x/seatunnel?mirror=1", "https://x/seatunnel#top", "https://x/seatunnel?",
            "https://x/seatunnel#", "https://x/seatunnel/?",
            # Whitespace, control or non-ASCII characters, and backslashes inside the URL.
            "https://x/sea tunnel", "https://x/sea\ntunnel", "https://x/sea\ttunnel",
            "https://x/sea\rtunnel", "https://x/sea\x00tunnel", "https://x\\seatunnel",
            "https://x/séatunnel",
            # Broken percent escapes, and brackets outside an IPv6 host.
            "https://x/sea%zztunnel", "https://x/seatunnel%", "https://x/seatunnel%2",
            "https://x/sea[tunnel]",
        ):
            with self.subTest(base_url=base_url):
                with self.assertRaises(DistributionError):
                    distribution_url("3.0.0", base_url)

    def test_non_string_base_url_raises(self):
        for base_url in (None, 3, b"https://x/seatunnel"):
            with self.subTest(base_url=base_url):
                with self.assertRaises(DistributionError):
                    distribution_url("3.0.0", base_url)

    def test_errors_explain_the_problem(self):
        cases = {
            "http://x/seatunnel": "https://",
            "https://:443/seatunnel": "no host",
            "https://x/seatunnel#": "'#'",
            "https://x/sea tunnel": "spaces",
            "https://host:abc/seatunnel": "Invalid distribution base URL",
            "https://user@host/seatunnel": "credentials",
        }
        for base_url, expected in cases.items():
            with self.subTest(base_url=base_url):
                with self.assertRaises(DistributionError) as ctx:
                    distribution_url("3.0.0", base_url)
                self.assertIn(expected, str(ctx.exception))

    def test_credentials_are_not_shown_in_errors(self):
        for base_url in ("https://alice:secret@host/seatunnel",
                         "https://alice:secret@host/sea tunnel",
                         "http://alice:secret@host/seatunnel"):
            with self.subTest(base_url=base_url):
                with self.assertRaises(DistributionError) as ctx:
                    distribution_url("3.0.0", base_url)
                self.assertNotIn("secret", str(ctx.exception))
                self.assertNotIn("alice", str(ctx.exception))


class TestCacheDir(unittest.TestCase):
    # The platform, the home directory and the environment are faked, so every platform branch
    # runs on any OS. CI runs on Linux, so there it checks which branch is taken and how the
    # path is built, with Linux path rules; Windows path rules are covered by running this
    # suite on Windows.

    def setUp(self):
        # Absolute paths of the host OS, so the tests behave the same on every OS.
        self.root = os.path.join(tempfile.gettempdir(), "seatunnel-cache-test")
        self.home = os.path.join(self.root, "alice")
        self.filesystem_root = os.path.abspath(os.sep)

    @contextlib.contextmanager
    def _environment(self, platform, home=None, **env):
        """Fake the platform, the home directory and the cache related environment."""
        home = home or self.home
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(distribution.sys, "platform", platform))
            stack.enter_context(mock.patch("os.path.expanduser", return_value=home))
            stack.enter_context(
                mock.patch.dict(os.environ, {"XDG_CACHE_HOME": "", "LOCALAPPDATA": ""})
            )
            stack.enter_context(mock.patch.dict(os.environ, env))
            yield

    def test_linux_uses_the_default_cache_directory(self):
        with self._environment("linux"):
            self.assertEqual(
                os.path.join(self.home, ".cache", "seatunnel", "3.0.0"), cache_dir("3.0.0")
            )

    def test_linux_honors_xdg_cache_home(self):
        xdg = os.path.join(self.root, "xdg")
        with self._environment("linux", XDG_CACHE_HOME=xdg):
            self.assertEqual(os.path.join(xdg, "seatunnel", "3.0.0"), cache_dir("3.0.0"))

    def test_macos_uses_the_library_caches_directory(self):
        with self._environment("darwin"):
            self.assertEqual(
                os.path.join(self.home, "Library", "Caches", "seatunnel", "3.0.0"),
                cache_dir("3.0.0"),
            )

    def test_windows_uses_localappdata(self):
        local = os.path.join(self.root, "local")
        with self._environment("win32", LOCALAPPDATA=local):
            self.assertEqual(os.path.join(local, "seatunnel", "3.0.0"), cache_dir("3.0.0"))

    def test_windows_without_localappdata_falls_back_to_the_home_directory(self):
        with self._environment("win32"):
            self.assertEqual(
                os.path.join(self.home, "AppData", "Local", "seatunnel", "3.0.0"),
                cache_dir("3.0.0"),
            )

    def test_relative_or_root_cache_variable_is_ignored(self):
        cases = (
            ("linux", "XDG_CACHE_HOME", (".cache",)),
            ("win32", "LOCALAPPDATA", ("AppData", "Local")),
        )
        for platform, variable, fallback in cases:
            for value in ("relative/cache", self.filesystem_root):
                with self.subTest(platform=platform, value=value):
                    with self._environment(platform, **{variable: value}):
                        self.assertEqual(
                            os.path.join(self.home, *fallback, "seatunnel", "3.0.0"),
                            cache_dir("3.0.0"),
                        )

    def test_home_at_the_filesystem_root_raises(self):
        # HOME=/ or HOME="" (expanduser returns "/" for both) would give every such user the
        # same /.cache/seatunnel directory.
        for platform, variable in (("linux", "HOME"), ("darwin", "HOME"),
                                   ("win32", "LOCALAPPDATA")):
            with self.subTest(platform=platform):
                with self._environment(platform, home=self.filesystem_root):
                    with self.assertRaises(DistributionError) as ctx:
                        cache_dir("3.0.0")
                self.assertIn("filesystem root", str(ctx.exception))
                self.assertIn(variable, str(ctx.exception))

    def test_unresolved_home_raises_and_names_the_variable_to_set(self):
        # expanduser returns "~" unchanged when HOME is unset and the user has no passwd entry.
        for platform, variable in (("linux", "XDG_CACHE_HOME or HOME"), ("darwin", "HOME"),
                                   ("win32", "LOCALAPPDATA or USERPROFILE")):
            with self.subTest(platform=platform):
                with self._environment(platform, home="~"):
                    with self.assertRaises(DistributionError) as ctx:
                        cache_dir("3.0.0")
                self.assertIn(f"Set {variable}", str(ctx.exception))

    def test_cache_variable_works_without_a_home_directory(self):
        cache = os.path.join(self.root, "cache")
        for platform, variable in (("linux", "XDG_CACHE_HOME"), ("win32", "LOCALAPPDATA")):
            with self.subTest(platform=platform):
                with self._environment(platform, home="~", **{variable: cache}):
                    self.assertEqual(
                        os.path.join(cache, "seatunnel", "3.0.0"), cache_dir("3.0.0")
                    )

    def test_versions_are_isolated_from_each_other(self):
        with self._environment("linux"):
            first, second = cache_dir("3.0.0"), cache_dir("3.0.1")
        self.assertNotEqual(first, second)
        self.assertEqual(os.path.dirname(first), os.path.dirname(second))

    def test_users_are_isolated_from_each_other(self):
        with self._environment("linux", home=os.path.join(self.root, "alice")):
            alice = cache_dir("3.0.0")
        with self._environment("linux", home=os.path.join(self.root, "bob")):
            bob = cache_dir("3.0.0")
        self.assertNotEqual(alice, bob)
        self.assertTrue(alice.startswith(os.path.join(self.root, "alice") + os.sep))
        self.assertTrue(bob.startswith(os.path.join(self.root, "bob") + os.sep))

    def test_version_cannot_name_a_directory_outside_the_cache(self):
        with self._environment("linux"):
            cache_root = os.path.dirname(cache_dir("3.0.0"))
            for version in ("..", ".", "../../etc", "3.0/../..", "/abs", "a/b", "nul"):
                with self.subTest(version=version):
                    with self.assertRaises(DistributionError):
                        cache_dir(version)
            self.assertEqual(cache_root, os.path.dirname(cache_dir("3.0.0")))

    def test_invalid_version_raises(self):
        with self._environment("linux"):
            for version in ("", None, 3, "3.0 beta"):
                with self.subTest(version=version):
                    with self.assertRaises(DistributionError):
                        cache_dir(version)

    def test_cache_dir_is_absolute_and_not_created(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self._environment("linux", home=tmp):
                path = cache_dir("3.0.0")
            self.assertTrue(os.path.isabs(path))
            self.assertFalse(os.path.exists(path))
            self.assertEqual([], os.listdir(tmp))


if __name__ == "__main__":
    unittest.main()

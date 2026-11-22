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

    def test_custom_base_url_is_used_and_trailing_slash_is_ignored(self):
        expected = (
            "https://archive.apache.org/dist/seatunnel/2.3.9/apache-seatunnel-2.3.9-bin.tar.gz"
        )
        self.assertEqual(
            expected, distribution_url("2.3.9", "https://archive.apache.org/dist/seatunnel")
        )
        self.assertEqual(
            expected, distribution_url("2.3.9", "https://archive.apache.org/dist/seatunnel/")
        )

    def test_invalid_versions_raise(self):
        for version in ("", "  ", ".", "..", "../x", "-3.0.0", "3.0.", "3.0-", "3.0/1",
                        "3.0\\1", "3.0 beta", "3.0;rm", "3.0?x=1", "3.0#frag", None, 3):
            with self.subTest(version=version):
                with self.assertRaises(DistributionError):
                    distribution_url(version)

    def test_single_character_version_is_accepted(self):
        self.assertTrue(distribution_url("3").endswith("/3/apache-seatunnel-3-bin.tar.gz"))

    def test_invalid_base_url_raises(self):
        for base_url in ("http://downloads.apache.org/seatunnel", "ftp://x/y", "https://",
                         "https:///seatunnel", "https://x/seatunnel?mirror=1",
                         "https://x/seatunnel#top", "downloads.apache.org/seatunnel", "",
                         None):
            with self.subTest(base_url=base_url):
                with self.assertRaises(DistributionError) as ctx:
                    distribution_url("3.0.0", base_url)
                self.assertIn("https://", str(ctx.exception))


class TestCacheDir(unittest.TestCase):
    def setUp(self):
        # Absolute paths of the host OS, so the tests behave the same on Linux, macOS and
        # Windows while sys.platform is faked.
        self.root = os.path.join(tempfile.gettempdir(), "seatunnel-cache-test")
        self.home = os.path.join(self.root, "alice")

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

    def test_linux_ignores_a_relative_xdg_cache_home(self):
        with self._environment("linux", XDG_CACHE_HOME="relative/cache"):
            self.assertEqual(
                os.path.join(self.home, ".cache", "seatunnel", "3.0.0"), cache_dir("3.0.0")
            )

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

    def test_relative_cache_location_raises(self):
        with self._environment("win32", LOCALAPPDATA="relative"):
            with self.assertRaises(DistributionError) as ctx:
                cache_dir("3.0.0")
            self.assertIn("absolute", str(ctx.exception))

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
            for version in ("..", ".", "../../etc", "3.0/../..", "/abs", "a/b"):
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

    def test_cache_dir_is_not_inside_the_installed_package(self):
        # Uses the real platform and environment, not the fakes above.
        package_dir = os.path.dirname(os.path.dirname(os.path.abspath(distribution.__file__)))
        path = cache_dir("3.0.0")
        self.assertTrue(os.path.isabs(path))
        self.assertFalse(path.startswith(package_dir + os.sep))


if __name__ == "__main__":
    unittest.main()

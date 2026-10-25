#!/usr/bin/env bash
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

echo "=============================================================================="
echo "Freeing up disk space on CI system"
echo "=============================================================================="

echo "Listing 100 largest packages"
dpkg-query -Wf '${Installed-Size}\t${Package}\n' | sort -n | tail -n 100
df -h

echo "Removing large directories in the background"
# These directories are not managed by dpkg, so removing them cannot race with
# the apt-get removals below. Run them in the background so the (slow) rm of the
# large Android SDK etc. overlaps with the (slow) apt-get transactions instead of
# running after them. The exact same paths are removed as before; only the
# ordering changes. dotnet/powershell are intentionally excluded here because
# they also have apt packages and are removed after the apt step to avoid racing
# apt's package post-removal scripts.
(
  sudo rm -rf /usr/local/graalvm/
  sudo rm -rf /usr/local/.ghcup/
  sudo rm -rf /usr/local/share/chromium
  sudo rm -rf /usr/local/share/boost
  sudo rm -rf /usr/local/lib/android
  sudo rm -rf /usr/local/lib/node_modules
  sudo rm -rf /opt/hostedtoolcache/CodeQL
  sudo rm -rf /opt/ghc
) &
rm_dirs_pid=$!

echo "Removing large packages"
# Each apt-get remove is intentionally a separate invocation: apt treats multiple
# patterns as a single transaction, so one non-matching pattern would abort the
# whole command and remove nothing. Keeping them separate preserves the original
# best-effort behavior where an unmatched pattern is tolerated.
sudo apt-get remove -y '^dotnet-.*'
sudo apt-get remove -y '^llvm-.*'
sudo apt-get remove -y 'php.*'
sudo apt-get remove -y '^mongodb-.*'
sudo apt-get remove -y '^mysql-.*'
sudo apt-get remove -y azure-cli google-cloud-sdk hhvm google-chrome-stable firefox powershell mono-devel libgl1-mesa-dri
sudo apt-get autoremove -y
sudo apt-get clean

# Wait for the background directory removals to finish before removing paths that
# overlap with apt-managed packages, so we never race apt's post-removal scripts.
wait "$rm_dirs_pid"

sudo rm -rf /usr/share/dotnet/
sudo rm -rf /usr/local/share/powershell

echo "Disk space after cleanup:"
df -h

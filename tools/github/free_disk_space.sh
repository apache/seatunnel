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


log_time_and_space() {
    local operation=$1
    local start_time=$2
    local start_space=$3
    # shellcheck disable=SC2155
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    # shellcheck disable=SC2155
    local end_space=$(df -P / | tail -n 1 | awk '{print $4}')
    local freed_space=$((end_space - start_space))
    # shellcheck disable=SC2155
    local freed_gb=$(echo "scale=2; $freed_space / 1024 / 1024" | bc)
    echo "------------------------------------------------------------------------------"
    echo "Operation: $operation"
    echo "Time taken: $duration seconds"
    echo "Freed disk space: $freed_gb GB"
    echo "------------------------------------------------------------------------------"
    echo
}

get_available_space() {
    df -P / | tail -n 1 | awk '{print $4}'
}

echo "=============================================================================="
echo "Freeing up disk space on CI system"
echo "=============================================================================="
df -h

# List 100 largest packages
start_time=$(date +%s)
start_space=$(get_available_space)
echo "Listing 100 largest packages"
dpkg-query -Wf '${Installed-Size}\t${Package}\n' | sort -nr | head -n 100

# Uninstall the faster ones first to preload and improve overall unloading efficiency.
# Clean up MongoDB
start_time=$(date +%s)
start_space=$(get_available_space)
sudo apt-get remove -y --purge '^mongodb-.*' > /dev/null 2>&1
log_time_and_space "Remove MongoDB related packages" $start_time $start_space

# Clean up .NET
start_time=$(date +%s)
start_space=$(get_available_space)
sudo apt-get remove -y --purge '^dotnet-.*' > /dev/null 2>&1
log_time_and_space "Remove .NET related packages" $start_time $start_space

# Clean up LLVM
start_time=$(date +%s)
start_space=$(get_available_space)
sudo apt-get remove -y --purge '^llvm-.*' > /dev/null 2>&1
log_time_and_space "Remove LLVM related packages" $start_time $start_space

# Clean up MySQL
start_time=$(date +%s)
start_space=$(get_available_space)
sudo apt-get remove -y --purge '^mysql-.*' > /dev/null 2>&1
log_time_and_space "Remove MySQL related packages" $start_time $start_space

# Clean up large packages
packages_to_check="ruby3.2-doc powershell azure-cli google-cloud-sdk hhvm google-chrome-stable firefox mono-devel libgl1-mesa-dri"
for package in $packages_to_check; do
  start_time=$(date +%s)
  start_space=$(get_available_space)
  if dpkg -l | grep -q "$package"; then
      sudo apt-get -o APT::Install-Suggests="false" remove -y --purge "$package" > /dev/null 2>&1
      log_time_and_space "Remove $package packages" $start_time $start_space
  fi
done

# Clean up apt cache
start_time=$(date +%s)
start_space=$(get_available_space)
sudo apt-get autoremove -y > /dev/null 2>&1
sudo apt-get clean > /dev/null 2>&1
log_time_and_space "Remove apt cache" $start_time $start_space

# Clean up Android directories
#start_time=$(date +%s)
#start_space=$(get_available_space)
#sudo nohup rm -rf /usr/local/lib/android > /dev/null 2>&1 &
#log_time_and_space "Remove android directories" $start_time $start_space

# Clean up large directories
directories=(
    "/usr/local/.ghcup/"
    "/usr/share/dotnet/"
    "/usr/local/graalvm/"
    "/usr/local/share/powershell"
    "/usr/local/share/chromium"
    "/usr/local/share/boost"
    "/usr/local/lib/node_modules"
    "/opt/hostedtoolcache/CodeQL"
    "/opt/ghc"
)
start_time=$(date +%s)
start_space=$(get_available_space)
sudo bash -c 'for dir in "${@}"; do [ -d "$dir" ] && rm -rf "$dir" & done; wait' _ "${directories[@]}"
log_time_and_space "Remove other large directories" $start_time $start_space

echo "=============================================================================="
echo "Disk cleanup completed"
echo "=============================================================================="
df -h
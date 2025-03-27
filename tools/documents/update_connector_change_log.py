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
import subprocess
import html
from packaging.version import Version
from pathlib import Path


def generate_log_info():
    directory = os.path.dirname(os.path.abspath(Path(__file__).parent.parent))
    connector_v2 = os.path.join(directory, 'seatunnel-connectors-v2')

    connector_changes = {}
    for root, dirs, files in os.walk(connector_v2):
        for d in dirs:
            if d.startswith('connector-'):
                prs = get_git_changes(os.path.join(root, d))
                if prs.__len__() > 0:
                    connector_changes[d] = prs

    return connector_changes


def get_git_changes(directory):
    result = subprocess.run(['git', 'log', '--pretty=format:%s%n'
                                           'https://github.com/apache/seatunnel/commit/%h%n' '%h',
                             '--',
                             directory],
                            cwd=directory, stdout=subprocess.PIPE)
    logs = result.stdout.decode('utf-8').splitlines()

    prs = []
    for i in range(0, len(logs), 3):
        prs.append((logs[i], logs[i + 1], logs[i + 2]))

    return prs


def get_tag_commit_list():
    directory = os.path.dirname(os.path.abspath(Path(__file__).parent.parent))
    result = subprocess.run(['git', 'fetch', 'https://github.com/apache/seatunnel.git', '--tags', '--force'],
                            cwd=directory, stdout=subprocess.PIPE)
    if result.returncode != 0:
        raise RuntimeError("Failed to fetch tags")

    result = subprocess.run(['git', 'tag'],
                            cwd=directory, stdout=subprocess.PIPE)
    if result.returncode != 0:
        raise RuntimeError("Failed to fetch tags")

    tags = result.stdout.decode('utf-8').splitlines()
    # Only consider tags starting with 2. for now
    tags = [tag for tag in tags if tag.startswith('2.')]
    sorted_versions = sorted(tags, key=Version, reverse=True)

    commit_version_map = {}
    for version in sorted_versions:
        result = subprocess.run(['git', 'log', version, '--pretty=format:%h'],
                                cwd=directory, stdout=subprocess.PIPE)
        if result.returncode != 0:
            raise RuntimeError("Failed to fetch tag logs")
        commits = result.stdout.decode('utf-8').splitlines()
        for commit in commits:
            commit_version_map[commit] = version

    return commit_version_map


def get_current_branch_name():
    directory = os.path.dirname(os.path.abspath(Path(__file__).parent.parent))
    result = subprocess.run(['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
                            cwd=directory, stdout=subprocess.PIPE)
    if result.returncode != 0:
        raise RuntimeError("Failed to fetch release")
    return result.stdout.decode('utf-8').strip()


def main():
    changes = generate_log_info()
    commit_version_map = get_tag_commit_list()
    in_release = get_current_branch_name().endswith('-release')
    directory = os.path.dirname(os.path.abspath(Path(__file__).parent.parent))
    changelog_dir = os.path.join(directory, 'docs', 'en', 'connector-v2', 'changelog')
    zh_changelog_dir = os.path.join(directory, 'docs', 'zh', 'connector-v2', 'changelog')
    for connector, prs in changes.items():
        write_commit(connector, prs, changelog_dir, commit_version_map, in_release)
        write_commit(connector, prs, zh_changelog_dir, commit_version_map, in_release)


def write_commit(connector, prs, changelog_dir, commit_version_map, in_release):
    with open(changelog_dir + '/' + connector + '.md', 'w') as file:
        file.write('<details><summary> Change Log </summary>\n\n')
        file.write('| Change | Commit | Version |\n')
        file.write('| --- | --- | --- |\n')
        for pr in prs:
            message = html.escape(pr[0])
            if pr[2] in commit_version_map:
                if not message.startswith('[maven-release-plugin]'):
                    file.write('|' + message + '|' + pr[1] + '|' + commit_version_map[pr[2]] + '|\n')
            else:
                if not in_release:
                    file.write('|' + message + '|' + pr[1] + '| dev |\n')
        file.write('\n</details>\n')
        file.close()


if __name__ == "__main__":
    main()

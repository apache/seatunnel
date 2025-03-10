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
from pathlib import Path


def generate_log_info():
    directory = os.path.dirname(os.path.abspath(Path(__file__).parent.parent))
    connector_v2 = os.path.join(directory, 'seatunnel-connectors-v2')

    result = subprocess.run(['git', 'fetch', 'https://github.com/apache/seatunnel.git', '--tags', '--force'],
                            cwd=directory, stdout=subprocess.PIPE)
    if result.returncode != 0:
        print("Failed to fetch tags")
        return

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
                                           'https://github.com/apache/seatunnel/commit/%h',
                             '--',
                             directory],
                            cwd=directory, stdout=subprocess.PIPE)
    logs = result.stdout.decode('utf-8').splitlines()

    prs = []
    for i in range(0, len(logs), 2):
        prs.append((logs[i], logs[i + 1]))

    return prs


def main():
    changes = generate_log_info()
    directory = os.path.dirname(os.path.abspath(Path(__file__).parent.parent))
    changelog_dir = os.path.join(directory, 'docs', 'en', 'connector-v2', 'changelog')
    zh_changelog_dir = os.path.join(directory, 'docs', 'zh', 'connector-v2', 'changelog')
    for connector, prs in changes.items():
        write_commit(connector, prs, changelog_dir)
        write_commit(connector, prs, zh_changelog_dir)


def write_commit(connector, prs, changelog_dir):
    with open(changelog_dir + '/' + connector + '.md', 'w') as file:
        file.write('| Change | Commit |\n')
        file.write('| --- | --- |\n')
        for pr in prs:
            file.write('|' + pr[0] + '|' + pr[1] + '|\n')
        file.close()


if __name__ == "__main__":
    main()

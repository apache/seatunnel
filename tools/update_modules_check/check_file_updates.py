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

# !/usr/bin/python
import argparse
import git
import json
import glob

def get_changed_files_between_branches(repo_path1, branch1, branch2, directorys):
    repo1 = git.Repo(repo_path1)

    commit1 = repo1.commit(branch1)
    commit2 = repo1.commit(branch2)

    diff = commit1.diff(commit2, create_patch=True)

    changed_files = []

    for file_diff in diff:
        for directory in directorys:
            if file_diff.a_path != file_diff.b_path:
                if file_diff.b_path is not None and glob.fnmatch.fnmatch(file_diff.b_path, directory):
                    changed_files.append(file_diff.b_path)

                if file_diff.a_path is not None and glob.fnmatch.fnmatch(file_diff.a_path, directory):
                    changed_files.append(file_diff.a_path)
            else:
                if glob.fnmatch.fnmatch(file_diff.b_path, directory):
                    changed_files.append(file_diff.b_path)

    return changed_files

def get_deleted_files_between_branches(repo_path, branch1, branch2, directorys):
    deleted_files = []

    repo1 = git.Repo(repo_path)

    commit1 = repo1.commit(branch1)
    commit2 = repo1.commit(branch2)

    diff = commit1.diff(commit2, create_patch=True)

    for file_diff in diff:
        for directory in directorys:
            if file_diff.a_path is not None and file_diff.b_path is None and glob.fnmatch.fnmatch(file_diff.a_path, directory):
                deleted_files.append(file_diff.a_path)

    return deleted_files

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare changes in a specified directory between two branches in different repositories.")
    parser.add_argument("type", help="ua will return update and add files, d will return delete files")
    parser.add_argument("repo_path", help="Path to the first local Git repository")
    parser.add_argument("branch1", help="Name of the first branch to compare")
    parser.add_argument("branch2", help="Name of the second branch to compare")
    parser.add_argument("directorys", nargs="+", help="Directory to compare")

    args = parser.parse_args()

    repo = git.Repo(args.repo_path)

    ref1 = repo.refs[args.branch1]
    ref2 = repo.refs[args.branch2]

    common_ancestor = repo.merge_base(ref1, ref2)[0].hexsha

    if args.type == 'ua':
        changed_files = get_changed_files_between_branches(args.repo_path, common_ancestor, args.branch2, args.directorys)
        if changed_files:
            print('true')
            result = json.dumps(changed_files, indent=None)
            print(result)
        else:
            print('false')
            result = json.dumps([], indent=None)
            print(result)
    else:
        delete_files = get_deleted_files_between_branches(args.repo_path, common_ancestor, args.branch2, args.directorys)
        if delete_files:
            print('true')
            result = json.dumps(delete_files, indent=None)
            print(result)
        else:
            print('false')
            result = json.dumps([], indent=None)
            print(result)
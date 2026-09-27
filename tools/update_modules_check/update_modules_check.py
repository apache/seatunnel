#!/usr/bin/env python3
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

import json
import sys
import zlib


# Selected from the module durations in GitHub Actions run 31611542978, where
# the resulting seven shards were estimated at 79.1-89.6 minutes. The stable
# hash keeps existing modules in the same shard when modules are added or removed,
# without maintaining per-module durations or assignments.
#
# Re-tune only when the module set or durations change enough to unbalance the
# shards. Export the latest per-module durations as {module: seconds}, evaluate
# candidate seeds by assigning each module with the crc32 expression below, and
# choose the seed that minimizes (maximum shard duration, shard-duration spread).
# Update test_historical_seed_assignments_are_preserved with the seed so an
# accidental reshuffle cannot silently change the CI matrix.
_FULL_CONNECTOR_IT_SHARD_SEED = "37709"

# Connector modules handled by jobs outside the shared connector shards. Add a
# module here only after its dedicated job handles API, engine, and direct changes
# to that module.
ALL_CONNECTORS_REQUIRED_DEDICATED_SHARD_MODULES = (
    "connector-jdbc-e2e",
    "connector-kafka-e2e",
    "connector-rocketmq-e2e",
    "connector-kudu-e2e",
    "connector-amazonsqs-e2e",
    "connector-google-pubsub-e2e",
    "connector-doris-e2e",
    "connector-paimon-e2e",
    "connector-cdc-oracle-e2e",
    "connector-file-local-e2e",
    "connector-file-sftp-e2e",
    "connector-redis-e2e",
    "connector-elasticsearch-e2e",
    "connector-cdc-mysql-e2e",
    "connector-iceberg-e2e",
    "connector-hbase-e2e",
    "connector-sensorsdata-e2e",
)

# These suites have dedicated jobs in backend.yml, but they are not listed by
# seatunnel-connector-v2-e2e's project.modules input.
ALL_CONNECTORS_OPTIONAL_DEDICATED_SHARD_MODULES = (
    "connector-seatunnel-e2e-base",
    "connector-console-seatunnel-e2e",
    "seatunnel-edge-agent-e2e",
)

ALL_CONNECTORS_DEDICATED_SHARD_MODULES = (
    ALL_CONNECTORS_REQUIRED_DEDICATED_SHARD_MODULES
    + ALL_CONNECTORS_OPTIONAL_DEDICATED_SHARD_MODULES
)

# The JDBC aggregate is excluded from full shards because its dedicated part
# jobs own full runs, but direct JDBC changes still use the updated leaf shards.
_CONNECTOR_IT_MODULES_WITH_DEDICATED_JOB = set(
    ALL_CONNECTORS_REQUIRED_DEDICATED_SHARD_MODULES
) - {"connector-jdbc-e2e"}


def get_cv2_modules(files):
    get_modules(files, 1, "connector-", "seatunnel-connectors-v2")


def get_cv2_e2e_modules(files):
    get_modules(files, 2, "connector-", "seatunnel-connector-v2-e2e")


def get_engine_modules(files):
    # We don't run all connector e2e when engine module update
    print(",connector-seatunnel-e2e-base,connector-console-seatunnel-e2e")


def get_engine_e2e_modules(files):
    get_modules(files, 2, "connector-", "seatunnel-engine-e2e")


def get_modules(files, index, start_pre, root_module):
    update_files = json.loads(files)
    modules_name_set = set([])
    for file in update_files:
        names = file.split('/')
        module_name = names[index]
        if module_name.startswith(start_pre):
            modules_name_set.add(module_name)

        if len(names) > index + 1 and names[index + 1].startswith(start_pre):
            modules_name_set.add(names[index + 1])

    output_module = ""
    if len(modules_name_set) > 0:
        for module in modules_name_set:
            output_module = output_module + "," + module

    else:
        output_module = output_module + "," + root_module

    print(output_module)


def replace_comma_to_commacolon(modules_str):
    modules_str = modules_str.replace(",", ",:")
    modules_str = ":" + modules_str
    print(modules_str)


def modules_to_json(modules):
    return json.dumps(
        [module.lstrip(":") for module in modules.split(",") if module]
    )


def get_sub_modules(file):
    output = ""
    with open(file, 'r', encoding='utf-8') as f:
        for line in f.readlines():
            line = line.replace(" ", "")
            if line.startswith("<string>"):
                line = line.replace(" ", "").replace("<string>", "").replace("</string>", "").replace("\n", "")
                output = output + "," + line
    print(output)


def get_dependency_tree_includes(modules_str):
    modules = modules_str.split(',')
    output = ""
    for module in modules:
        output = ",org.apache.seatunnel:" + module + output

    output = output[1:len(output)]
    output = "-Dincludes=" + output
    print(output)


def get_final_it_modules(file):
    output = ""
    with open(file, 'r', encoding='utf-8') as f:
        for line in f.readlines():
            if line.startswith("org.apache.seatunnel"):
                con = line.split(":")
                # find all e2e modules
                if con[2] == "jar" and "-e2e" in con[1] and "transform" not in con[1]:
                    output = output + "," + ":" + con[1]
    output = output[1:len(output)]
    print(output)


def get_final_ut_modules(file):
    output = ""
    with open(file, 'r', encoding='utf-8') as f:
        for line in f.readlines():
            if line.startswith("org.apache.seatunnel"):
                con = line.split(":")
                # find all e2e modules
                if con[2] == "jar":
                    output = output + "," + ":" + con[1]

    output = output[1:len(output)]
    print(output)


def remove_deleted_modules(pl_modules, deleted_modules):
    pl_modules_arr = pl_modules.replace(":", "").split(",")
    deleted_modules_arr = deleted_modules.split(",")
    output = ""
    for module in pl_modules_arr:
        if deleted_modules_arr.count(module) == 0:
            output = output + ",:" + module

    output = output[1:len(output)]
    print(output)


def get_deleted_modules(files):
    update_files = json.loads(files)
    modules_name_set = set([])
    for file in update_files:
        names = file.split('/')
        module_name = names[len(names) - 2]
        modules_name_set.add(module_name)
    output_module = ""
    if len(modules_name_set) > 0:
        for module in modules_name_set:
            output_module = output_module + "," + module

    output_module = output_module[1:len(output_module)]
    print(output_module)


def _filter_shared_it_modules(modules, extra_exclusions=()):
    excluded_modules = _CONNECTOR_IT_MODULES_WITH_DEDICATED_JOB | set(
        extra_exclusions
    )
    return [
        module
        for module in dict.fromkeys(modules)
        if module and module not in excluded_modules
    ]


def filter_dedicated_shard_modules(modules, dedicated_modules, fail_on_missing):
    """Remove modules owned by dedicated jobs and optionally detect workflow drift."""
    module_set = set(modules)
    if fail_on_missing:
        missing_modules = [
            module for module in dedicated_modules if module not in module_set
        ]
        if missing_modules:
            raise ValueError(
                "Missing dedicated shard modules from all-connectors input: "
                + ",".join(missing_modules)
            )

    dedicated_modules_set = set(dedicated_modules)
    return [module for module in modules if module not in dedicated_modules_set]


def split_full_connector_it_modules(modules, total_num):
    if total_num <= 0:
        raise ValueError(f"total shard count must be positive, got {total_num}")

    shards = [[] for _ in range(total_num)]
    for module in sorted(set(modules)):
        shard_key = f"{_FULL_CONNECTOR_IT_SHARD_SEED}:{module}".encode("utf-8")
        shard = zlib.crc32(shard_key) % total_num
        shards[shard].append(module)
    return shards


def build_sub_it_modules(modules, total_num, current_num):
    """Build one stable full connector shard after applying ownership exclusions."""
    total_num = int(total_num)
    current_num = int(current_num)
    if total_num <= 0:
        raise ValueError(f"total shard count must be positive, got {total_num}")
    if not 0 <= current_num < total_num:
        raise ValueError(
            f"shard index {current_num} out of range [0, {total_num})"
        )

    modules_arr = [module for module in dict.fromkeys(modules.split(",")) if module]
    modules_arr = filter_dedicated_shard_modules(
        modules_arr, ALL_CONNECTORS_REQUIRED_DEDICATED_SHARD_MODULES, True
    )
    modules_arr = filter_dedicated_shard_modules(
        modules_arr, ALL_CONNECTORS_OPTIONAL_DEDICATED_SHARD_MODULES, False
    )
    shards = split_full_connector_it_modules(modules_arr, total_num)
    return ",".join(":" + module for module in shards[current_num])


def get_sub_it_modules(modules, total_num, current_num):
    print(build_sub_it_modules(modules, total_num, current_num))


def get_sub_update_it_modules(modules, total_num, current_num):
    final_modules = list()
    module_names = json.loads(modules)
    module_list = _filter_shared_it_modules(
        module_names,
        {
            "connector-seatunnel-e2e-base",
            "connector-console-seatunnel-e2e",
            "seatunnel-engine-k8s-e2e",
            "seatunnel-edge-agent-e2e",
        },
    )
    for i, module in enumerate(module_list):
        if len(module) > 0 and i % int(total_num) == int(current_num):
            final_modules.append(":" + module)
    print(",".join(final_modules))


def main(argv):
    if argv[1] == "cv2":
        get_cv2_modules(argv[2])
    elif argv[1] == "cv2-e2e":
        get_cv2_e2e_modules(argv[2])
    elif argv[1] == "engine":
        get_engine_modules(argv[2])
    elif argv[1] == "engine-e2e":
        get_engine_e2e_modules(argv[2])
    elif argv[1] == "tree":
        get_dependency_tree_includes(argv[2])
    elif argv[1] == "final_it":
        get_final_it_modules(argv[2])
    elif argv[1] == "final_ut":
        get_final_ut_modules(argv[2])
    elif argv[1] == "replace":
        replace_comma_to_commacolon(argv[2])
    elif argv[1] == "json":
        print(modules_to_json(argv[2]))
    elif argv[1] == "sub":
        get_sub_modules(argv[2])
    elif argv[1] == "delete":
        get_deleted_modules(argv[2])
    elif argv[1] == "rm":
        remove_deleted_modules(argv[2], argv[3])
    elif argv[1] == "sub_it_module":
        get_sub_it_modules(argv[2], argv[3], argv[4])
    elif argv[1] == "sub_update_it_module":
        get_sub_update_it_modules(argv[2], argv[3], argv[4])


if __name__ == "__main__":
    main(sys.argv)

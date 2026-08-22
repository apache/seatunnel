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
import json
import sys

# E2E suites that have their own dedicated workflow job in .github/workflows/backend.yml.
# The trailing comment records which job runs each module, so the sub-task <-> module
# mapping is visible at a glance. Keeping these out of the round-robin integration-test
# shards avoids running the same suite twice under the 20-job concurrency limit.
EXCLUDED_E2E_MODULES = {
    # --- individual connector suites -> their dedicated connector-*-it job ---
    "connector-redis-e2e",            # connector-redis-it
    "connector-file-local-e2e",       # connector-file-local-it
    "connector-file-sftp-e2e",        # connector-file-sftp-it
    "connector-amazonsqs-e2e",        # connector-amazonSqs-it
    "connector-kafka-e2e",            # connector-kafka-it
    "connector-rocketmq-e2e",         # connector-rocketmq-it
    "connector-doris-e2e",            # connector-doris-it
    "connector-paimon-e2e",           # connector-paimon-it
    "connector-cdc-oracle-e2e",       # connector-oracle-cdc-it
    "connector-kudu-e2e",             # connector-kudu-it
    "connector-sensorsdata-e2e",      # connector-sensorsdata-it
    "connector-http-e2e",             # connector-http-it
    "connector-hbase-e2e",            # connector-hbase-it
    "connector-mongodb-e2e",          # connector-mongodb-it
    "connector-cdc-mysql-e2e",        # connector-mysql-cdc-it
    "connector-elasticsearch-e2e",    # connector-elasticsearch-it
    "connector-clickhouse-e2e",       # connector-clickhouse-it
    # --- three iceberg suites -> one consolidated connector-iceberg-it job ---
    "connector-iceberg-e2e",          # connector-iceberg-it
    "connector-iceberg-hadoop3-e2e",  # connector-iceberg-it
    "connector-iceberg-s3-e2e",       # connector-iceberg-it
    # --- engine / edge-agent e2e bases and suites ---
    "connector-seatunnel-e2e-base",   # engine-v2-it
    "connector-console-seatunnel-e2e",# engine-v2-it
    "seatunnel-edge-agent-e2e",       # edge-agent-it
}


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


def build_sub_it_modules(modules, total_num, current_num):
    """
    Build one all-connectors shard while excluding suites with dedicated jobs.

    Heavy suites that already have their own workflow shard must stay out of the
    round-robin shards, otherwise CI runs them twice and wastes runner time.
    """
    modules_arr = list(dict.fromkeys(modules.split(",")))
    # The whole connector-jdbc-e2e module is split across the dedicated
    # jdbc-connectors-it-part-* jobs, so drop it here as well.
    excluded = EXCLUDED_E2E_MODULES | {"connector-jdbc-e2e"}
    modules_arr = [m for m in modules_arr if m not in excluded]
    output = ""
    for i, module in enumerate(modules_arr):
        if len(module) > 0 and i % int(total_num) == int(current_num):
            output = output + ",:" + module

    return output[1:len(output)]


def get_sub_it_modules(modules, total_num, current_num):
    print(build_sub_it_modules(modules, total_num, current_num))


def get_sub_update_it_modules(modules, total_num, current_num):
    final_modules = list()
    # :connector-jdbc-e2e-common,:connector-jdbc-e2e-part-1 --> connector-jdbc-e2e-common,:connector-jdbc-e2e-part-1
    modules = modules[1:]
    # connector-jdbc-e2e-common,:connector-jdbc-e2e-part-1 --> [connector-jdbc-e2e-common, connector-jdbc-e2e-part-1]
    module_list = list(dict.fromkeys(modules.split(",:")))
    # The engine-k8s e2e suite has its own engine-k8s-it job.
    excluded = EXCLUDED_E2E_MODULES | {"seatunnel-engine-k8s-e2e"}
    module_list = [m for m in module_list if m not in excluded]
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

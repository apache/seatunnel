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


def get_sub_it_modules(modules, total_num, current_num):
    modules_arr = modules.split(",")
    modules_arr.remove("connector-jdbc-e2e")
    modules_arr.remove("connector-kafka-e2e")
    modules_arr.remove("connector-rocketmq-e2e")
    modules_arr.remove("connector-kudu-e2e")
    modules_arr.remove("connector-amazonsqs-e2e")
    modules_arr.remove("connector-doris-e2e")
    modules_arr.remove("connector-paimon-e2e")
    modules_arr.remove("connector-cdc-oracle-e2e")
    output = ""
    for i, module in enumerate(modules_arr):
        if len(module) > 0 and i % int(total_num) == int(current_num):
            output = output + ",:" + module

    output = output[1:len(output)]
    print(output)


def get_sub_update_it_modules(modules, total_num, current_num):
    final_modules = list()
    # :connector-jdbc-e2e-common,:connector-jdbc-e2e-part-1 --> connector-jdbc-e2e-common,:connector-jdbc-e2e-part-1
    modules = modules[1:]
    # connector-jdbc-e2e-common,:connector-jdbc-e2e-part-1 --> [connector-jdbc-e2e-common, connector-jdbc-e2e-part-1]
    module_list = modules.split(",:")
    if "connector-kudu-e2e" in module_list:
        module_list.remove("connector-kudu-e2e")
    if "connector-amazonsqs-e2e" in module_list:
        module_list.remove("connector-amazonsqs-e2e")
    if "connector-kafka-e2e" in module_list:
        module_list.remove("connector-kafka-e2e")
    if "connector-rocketmq-e2e" in module_list:
        module_list.remove("connector-rocketmq-e2e")
    if "seatunnel-engine-k8s-e2e" in module_list:
        module_list.remove("seatunnel-engine-k8s-e2e")
    if "connector-doris-e2e" in module_list:
        module_list.remove("connector-doris-e2e")
    if "connector-paimon-e2e" in module_list:
        module_list.remove("connector-paimon-e2e")
    if "connector-cdc-oracle-e2e" in module_list:
        module_list.remove("connector-cdc-oracle-e2e")
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

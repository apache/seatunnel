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

#!/usr/bin/python
import json
import sys

def get_cv2_modules(files):
    get_modules(files, 1, "connector-", "seatunnel-connectors-v2")

def get_cv2_flink_e2e_modules(files):
    flink_e2e_modules = get_modules(files, 2, "connector-", "seatunnel-e2e/seatunnel-flink-connector-v2-e2e")
    flink_e2e_modules = flink_e2e_modules + ","

def get_cv2_spark_e2e_modules(files):
    get_modules(files, 2, "connector-", "seatunnel-e2e/seatunnel-spark-connector-v2-e2e")

def get_cv2_e2e_modules(files):
    get_modules(files, 2, "connector-", "seatunnel-e2e/seatunnel-connector-v2-e2e")

def get_engine_modules(files):
    get_modules(files, 1, "seatunnel-", "seatunnel-engine")

def get_engine_e2e_modules(files):
    get_modules(files, 2, "connector-", "seatunnel-e2e/seatunnel-engine-e2e")

def get_modules(files, index, start_pre, root_module):
    update_files = json.loads(files)
    modules_name_set = set([])
    for file in update_files:
        module_name = file.split('/')[index]
        if module_name.startswith(start_pre):
            modules_name_set.add(root_module + "/" + module_name)

    output_module = ""
    if len(modules_name_set) > 0:
        for module in modules_name_set:
            output_module = output_module + "," + module

    else:
        output_module = root_module

    return output_module

def main(argv):
    if argv[1] == "cv2":
        get_cv2_modules(argv[2])
    elif argv[1] == "cv2-e2e":
        get_cv2_e2e_modules(argv[2])
    elif argv[1] == "cv2-flink-e2e":
        get_cv2_flink_e2e_modules(argv[2])
    elif argv[1] == "cv2-spark-e2e":
        get_cv2_spark_e2e_modules(argv[2])
    elif argv[1] == "engine":
        get_engine_modules(argv[2])
    elif argv[1] == "engine-e2e":
        get_engine_e2e_modules(argv[2])

if __name__ == "__main__":
    main(sys.argv)
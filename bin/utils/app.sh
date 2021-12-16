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

# Get App Name of SeaTunnel
function getAppName {

    if [ "$#" -ne 1 ]; then
        echo "Illegal number of parameters"
        exit -1
    fi

    config_file_path=$1

    app_name_config=$(grep -i "spark.app.name" ${config_file_path})
    # remove all leading and trailing  whitespace
    app_name_config=$(echo ${app_name_config} | tr -d '[:space:]')

    DEFAULT_APP_NAME="SeaTunnel"
    APP_NAME=${DEFAULT_APP_NAME}

    if [[ ${app_name_config} == \#* ]]; then
        # spark.app.name is commented, we should ignore
        :
    else

        if [ "${app_name_config}" != "" ];then
            # split app_name from config
            APP_NAME=${app_name_config##*=}
            # remove quotes if exists
            APP_NAME=$(echo ${APP_NAME} | tr -d '"' | tr -d "'")
        fi
    fi

    echo ${APP_NAME}
}
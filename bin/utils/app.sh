#!/usr/bin/env bash
# Get App Name of Waterdrop
function getAppName {

    if [ "$#" -ne 1 ]; then
        echo "Illegal number of parameters"
        exit -1
    fi

    config_file_path=$1

    app_name_config=$(grep -i "spark.app.name" ${config_file_path})
    # remove all leading and trailing  whitespace
    app_name_config=$(echo ${app_name_config} | tr -d '[:space:]')

    DEFAULT_APP_NAME="Waterdrop"
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
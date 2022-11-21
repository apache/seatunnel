#!/bin/bash
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

#This script is used to download the connector plug-ins required during the running process. 
#All are downloaded by default. You can also choose what you need. 
#You only need to configure the plug-in name in config/plugin_config.

# get seatunnel home
SEATUNNEL_HOME=$(cd $(dirname $0);cd ../;pwd)

# connector default version is 2.2.0, you can also choose a custom version. eg: 2.1.2:  sh install-plugin.sh 2.1.2
version=2.2.0

if [ -n "$1" ]; then
    version="$1"
fi

echo "Install SeaTunnel connectors plugins, usage version is ${version}"

# create the connectors directory
if [ ! -d ${SEATUNNEL_HOME}/connectors ];
  then
      mkdir ${SEATUNNEL_HOME}/connectors
      echo "create connectors directory"
fi

# create the flink-sql connectors directory (for v1)
if [ ! -d ${SEATUNNEL_HOME}/connectors/flink-sql ];
  then
      mkdir ${SEATUNNEL_HOME}/connectors/flink-sql
      echo "create flink-sql connectors directory"
fi

# create the flink connectors directory (for v1)
if [ ! -d ${SEATUNNEL_HOME}/connectors/flink ];
  then
      mkdir ${SEATUNNEL_HOME}/connectors/flink
      echo "create flink connectors directory"
fi

# create the spark connectors directory (for v1)
if [ ! -d ${SEATUNNEL_HOME}/connectors/spark ];
  then
      mkdir ${SEATUNNEL_HOME}/connectors/spark
      echo "create spark connectors directory"
fi

# create the seatunnel connectors directory (for v2)
if [ ! -d ${SEATUNNEL_HOME}/connectors/seatunnel ];
  then
      mkdir ${SEATUNNEL_HOME}/connectors/seatunnel
      echo "create seatunnel connectors directory"
fi  

path=flink-sql

while read line; do
    # v1 connectors flink-sql
	  if [ "$line" = "--flink-sql-connectors--" ]
	    then
	  	 path=flink-sql
	  fi
	  # v1 connectors flink
	  if [ "$line" = "--flink-connectors--" ]
	    then
	  	 path=flink
	  fi
	  # v1 connectors spark
	  if [ "$line" = "--spark-connectors--" ]
	    then
	  	 path=spark
	  fi
	  # v2 connectors
	  if [ "$line" = "--connectors-v2--" ]
	    then
	  	 path=seatunnel
	  fi
    if  [ ${line:0:1} != "-" ] && [ ${line:0:1} != "#" ]
      	then
      		echo "install connector : " $line
      		${SEATUNNEL_HOME}/mvnw dependency:get -DgroupId=org.apache.seatunnel -DartifactId=${line} -Dversion=${version} -Ddest=${SEATUNNEL_HOME}/connectors/${path}
    fi

done < ${SEATUNNEL_HOME}/config/plugin_config
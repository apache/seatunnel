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

# connector default version is 2.2.0, you can also choose a custom version. eg: 2.1.2:  sh install-plugin.sh 2.1.2
version=2.2.0

if [ -n "$1" ]; then
    version="$1"
fi

echo "Install SeaTunnel connectors plugins, usage version is $version $1"

if [ ! -d connectors ];
  then
      mkdir connectors
      echo "create connectors directory"
fi      
if [ ! -d connectors/flink-sql ];
  then
      mkdir connectors/flink-sql
      echo "create flink-sql connectors directory"
fi
if [ ! -d connectors/flink ];
  then
      mkdir connectors/flink
      echo "create flink connectors directory"
fi 
if [ ! -d connectors/spark ];
  then
      mkdir connectors/spark
      echo "create spark connectors directory"
fi 
if [ ! -d connectors/seatunnel ];
  then
      mkdir connectors/seatunnel
      echo "create seatunnel connectors directory"
fi  

path=flink-sql

while read line; do
    if  [ ${line:0:1} != "-" ] && [ ${line:0:1} != "#" ]
      	then
      		echo "install connector : " $line
      		./mvnw dependency:get -DgroupId=org.apache.seatunnel -DartifactId=${line} -Dversion=${version} -Ddest=connectors/${path}
    fi
	  if [ "$line" = "--flink-sql-connectors--" ]
	    then 
	  	 path=flink-sql
	  fi	 
	  if [ "$line" = "--flink-connectors--" ]
	    then 
	  	 path=flink
	  fi	 	
	  if [ "$line" = "--spark-connectors--" ]
	    then 
	  	 path=spark
	  fi	 
	  if [ "$line" = "--connectors-v2--" ]
	    then 
	  	 path=seatunnel
	  fi	 

done < config/plugin_config
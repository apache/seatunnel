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

SEATUNNEL_DEFAULT_CLUSTER_NAME="seatunnel_default_cluster"
SHOW_USAGE="Usage: stop-seatunnel-cluster.sh [options]\n Options:\n       -cn, --cluster      The name of the cluster
 to shut down (default: $SEATUNNEL_DEFAULT_CLUSTER_NAME)\n        -h, --help          Show the usage message"
APP_MAIN="org.apache.seatunnel.core.starter.seatunnel.SeaTunnelServer"


if [ $# -ne 0 ]; then
  while true; do
    case "$1" in
      -cn|--cluster)
        shift
        CLUSTER_NAME="$1"
        break
        ;;
      -h|--help)
        echo -e $SHOW_USAGE
        exit 0
        ;;
      *)
        echo "Unknown option: $1, please use [-h | --help] to show options"
        exit 0
        ;;
    esac
  done
fi

if test -z $CLUSTER_NAME;then
   RES=$(ps -ef | grep $APP_MAIN | grep -v "\-cn\|\--cluster" | grep -v grep | awk '{print $2}')
   if test -z $RES;then
     echo "$SEATUNNEL_DEFAULT_CLUSTER_NAME is not running. Please check the correct name of the running cluster."
     exit 0
   fi
   kill $RES >/dev/null
else
   RES=$(ps -ef | grep $APP_MAIN | grep $CLUSTER_NAME | grep -v grep | awk '{print $2}')
   if test -z $RES;then
     echo "$CLUSTER_NAME is not running. Please check the correct name of the running cluster."
     exit 0
   fi
   kill $RES >/dev/null
fi
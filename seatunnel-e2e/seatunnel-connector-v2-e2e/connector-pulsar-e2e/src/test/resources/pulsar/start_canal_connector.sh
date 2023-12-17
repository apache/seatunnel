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

./bin/pulsar-admin source localrun \
   --archive ./connectors/pulsar-io-canal-2.3.0.nar \
   --classname org.apache.pulsar.io.canal.CanalStringSource \
   --tenant public \
   --namespace default \
   --name canal \
   --destination-topic-name test-cdc_mds \
   --source-config-file /pulsar/conf/canal-mysql-source-config.yaml \
   --parallelism 1

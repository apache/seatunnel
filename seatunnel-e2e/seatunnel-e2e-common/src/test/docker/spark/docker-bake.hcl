# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

group "default" {
  targets = ["spark-2.4.6", "spark-3.3.0"]
}

target "spark" {
  context    = "."
  dockerfile = "Dockerfile"
  platforms  = ["linux/amd64", "linux/arm64"]
}

target "spark-2.4.6" {
  inherits = ["spark"]
  args = {
    SPARK_VERSION  = "2.4.6"
    HADOOP_PROFILE = "hadoop2.7"
  }
  tags = ["apache/seatunnel:e2e-spark-2.4.6"]
}

target "spark-3.3.0" {
  inherits = ["spark"]
  args = {
    SPARK_VERSION  = "3.3.0"
    HADOOP_PROFILE = "hadoop3"
  }
  tags = ["apache/seatunnel:e2e-spark-3.3.0"]
}

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
  targets = ["spark-2-4-6", "spark-3-3-0"]
}

target "spark" {
  context    = "."
  dockerfile = "Dockerfile"
  platforms  = ["linux/amd64", "linux/arm64"]
}

target "spark-2-4-6" {
  inherits = ["spark"]
  args = {
    SPARK_VERSION  = "2.4.6"
    HADOOP_PROFILE = "hadoop2.7"
    SPARK_SHA512   = "3a9f401eda9b5749cdafd246b1d14219229c26387017791c345a23a65782fb8b25a302bf4ac1ed7c16a1fe83108e94e55dad9639a51c751d81c8c0534a4a9641"
  }
  tags = ["apache/seatunnel:e2e-spark-2.4.6"]
}

target "spark-3-3-0" {
  inherits = ["spark"]
  args = {
    SPARK_VERSION  = "3.3.0"
    HADOOP_PROFILE = "hadoop3"
    SPARK_SHA512   = "1e8234d0c1d2ab4462d6b0dfe5b54f2851dcd883378e0ed756140e10adfb5be4123961b521140f580e364c239872ea5a9f813a20b73c69cb6d4e95da2575c29c"
  }
  tags = ["apache/seatunnel:e2e-spark-3.3.0"]
}

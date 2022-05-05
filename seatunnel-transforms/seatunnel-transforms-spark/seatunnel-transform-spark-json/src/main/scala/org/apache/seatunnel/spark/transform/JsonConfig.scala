/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.spark.transform

object JsonConfig {
  val PLUGIN_NAME = "json"

  val FIELDS = "fields"
  val SOURCE_FILED = "source_field"
  val DEFAULT_SOURCE_FILED = "raw_message"
  val TARGET_FILED = "target_field"
  val SCHEMA_DIR = "schema_dir"
  val SCHEMA_FILE = "schema_file"
  val DEFAULT_SCHEMA_FILE = ""
}

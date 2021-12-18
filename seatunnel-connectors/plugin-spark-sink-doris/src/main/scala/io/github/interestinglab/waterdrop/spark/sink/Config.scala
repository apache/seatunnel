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

package io.github.interestinglab.waterdrop.spark.sink

object Config extends Serializable {

  val HOST = "host"
  val DATABASE = "database"
  val TABLE_NAME = "tableName"
  val USER = "user"
  val PASSWORD = "password"
  val BULK_SIZE = "bulk_size"
  val COLUMN_SEPARATOR = "column_separator"
  val ARGS_PREFIX = "doris."

  val CHARSET = "UTF-8"
  val BINARY_CT = "application/octet-stream"
  val CONTENT_TYPE = "text/plain"
  var TIMEOUT = 30000

  val CHECK_INT_ERROR = "please check bulk_size is larger than 0"
  val CHECK_USER_ERROR = "please check username and password at the same time"
  val CHECK_SUCCESS = "all check is success"

}

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

package org.apache.seatunnel.spark.common

object Constants extends Serializable {

  final val HOST = "host"
  final val PORT = "port"
  final val AUTH = "auth"
  final val DB_NUM = "db_num"
  final val KEYS_OR_KEY_PATTERN = "keys_or_key_pattern"
  final val PARTITION_NUM = "partition_num"
  final val TIMEOUT = "timeout"
  final val DATA_TYPE = "data_type"
  final val HASH_NAME = "hash_name"
  final val SET_NAME = "set_name"
  final val ZSET_NAME = "zset_name"
  final val LIST_NAME = "list_name"

  final val DEFAULT_HOST = "localhost"
  final val DEFAULT_PORT = 6379
  final val DEFAULT_AUTH = null
  final val DEFAULT_DB_NUM = 0
  final val DEFAULT_DATA_TYPE = "KV"
  final val DEFAULT_PARTITION_NUM = 3
  final val DEFAULT_TIMEOUT = 2000

}

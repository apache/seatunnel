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

package org.apache.seatunnel.spark.clickhouse

/**
 * Configurations for Clickhouse sink
 */
object Config extends Serializable {

  /**
   * Bulk size of clickhouse jdbc
   */
  val BULK_SIZE = "bulk_size"

  /**
   * Clickhouse jdbc retry time
   */
  val RETRY = "retry"

  /**
   * Clickhouse fields
   */
  val FIELDS = "fields"

  /**
   * Clickhouse server host
   */
  val HOST = "host"

  /**
   * Clickhouse table name
   */
  val TABLE = "table"

  /**
   * Clickhouse database name
   */
  val DATABASE = "database"

  /**
   * Clickhouse server username
   */
  val USERNAME = "username"

  /**
   * Clickhouse server password
   */
  val PASSWORD = "password"

  /**
   * Split mode when table is distributed engine
   */
  val SPLIT_MODE = "split_mode"

  /**
   * When split_mode is true, the sharding_key use for split
   */
  val SHARDING_KEY = "sharding_key"

  /**
   * The retry code when use clickhouse jdbc
   */
  val RETRY_CODES = "retry_codes"

  /**
   * ClickhouseFile sink connector used clickhouse-local program's path
   */
  val CLICKHOUSE_LOCAL_PATH = "clickhouse_local_path"

  /**
   * The method of copy Clickhouse file
   */
  val COPY_METHOD = "copy_method"

  /**
   * The size of each batch read temporary data into local file.
   */
  val TMP_BATCH_CACHE_LINE = "tmp_batch_cache_line"

  /**
   * Clickhouse server node is free-password.
   */
  val NODE_FREE_PASSWORD = "node_free_password"

  /**
   * The password of Clickhouse server node
   */
  val NODE_PASS = "node_pass"

  /**
   * The address of Clickhouse server node
   */
  val NODE_ADDRESS = "node_address"
}

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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.config;

public class Config {

    /**
     * Bulk size of clickhouse jdbc
     */
    public static final String BULK_SIZE = "bulk_size";

    /**
     * Clickhouse fields
     */
    public static final String FIELDS = "fields";

    public static final String SQL = "sql";

    /**
     * Clickhouse server host
     */
    public static final String HOST = "host";

    /**
     * Clickhouse table name
     */
    public static final String TABLE = "table";

    /**
     * Clickhouse database name
     */
    public static final String DATABASE = "database";

    /**
     * Clickhouse server username
     */
    public static final String USERNAME = "username";

    /**
     * Clickhouse server password
     */
    public static final String PASSWORD = "password";

    /**
     * Split mode when table is distributed engine
     */
    public static final String SPLIT_MODE = "split_mode";

    /**
     * When split_mode is true, the sharding_key use for split
     */
    public static final String SHARDING_KEY = "sharding_key";

    /**
     * ClickhouseFile sink connector used clickhouse-local program's path
     */
    public static final String CLICKHOUSE_LOCAL_PATH = "clickhouse_local_path";

    /**
     * The method of copy Clickhouse file
     */
    public static final String COPY_METHOD = "copy_method";

    /**
     * The size of each batch read temporary data into local file.
     */
    public static final String TMP_BATCH_CACHE_LINE = "tmp_batch_cache_line";

    /**
     * The password of Clickhouse server node
     */
    public static final String NODE_PASS = "node_pass";

    /**
     * The address of Clickhouse server node
     */
    public static final String NODE_ADDRESS = "node_address";

    public static final String CLICKHOUSE_PREFIX = "clickhouse.";

}

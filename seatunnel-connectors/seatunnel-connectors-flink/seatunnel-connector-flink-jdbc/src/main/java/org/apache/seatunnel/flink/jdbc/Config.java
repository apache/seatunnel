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

package org.apache.seatunnel.flink.jdbc;

import org.apache.seatunnel.flink.jdbc.sink.JdbcSink;
import org.apache.seatunnel.flink.jdbc.source.JdbcSource;

/**
 * Jdbc source {@link JdbcSource} and
 * sink {@link JdbcSink} configuration parameters
 */
public final class Config {

    /**
     * Parallelism of the source or sink
     */
    public static final String PARALLELISM = "parallelism";

    /**
     * Jdbc driver for source or sink
     */
    public static final String DRIVER = "driver";

    /**
     * Jdbc Url for source or sink
     */
    public static final String URL = "url";

    /**
     * Jdbc username for source or sink
     */
    public static final String USERNAME = "username";

    /**
     * Jdbc query for source or sink
     */
    public static final String QUERY = "query";

    /**
     * Jdbc password for source or sink
     */
    public static final String PASSWORD = "password";

    /**
     * Jdbc fetch size for source
     */
    public static final String SOURCE_FETCH_SIZE = "fetch_size";

    /**
     * Jdbc batch size for sink
     */
    public static final String SINK_BATCH_SIZE = "batch_size";

    /**
     * Jdbc batch interval for sink
     */
    public static final String SINK_BATCH_INTERVAL = "batch_interval";

    /**
     * Jdbc max batch retries for sink
     */
    public static final String SINK_BATCH_MAX_RETRIES = "batch_max_retries";

    /**
     * Jdbc partition column name
     */
    public static final String PARTITION_COLUMN = "partition_column";

    /**
     * Jdbc partition upper bound
     */
    public static final String PARTITION_UPPER_BOUND = "partition_upper_bound";

    /**
     * Jdbc partition lower bound
     */
    public static final String PARTITION_LOWER_BOUND = "partition_lower_bound";

    /**
     * Jdbc pre sql for sink
     */
    public static final String SINK_PRE_SQL = "pre_sql";

    /**
     * Jdbc post sql for sink
     */
    public static final String SINK_POST_SQL = "post_sql";

    /**
     * Jdbc ignore post sql exceptions for sink
     */
    public static final String SINK_IGNORE_POST_SQL_EXCEPTIONS = "ignore_post_sql_exceptions";
}

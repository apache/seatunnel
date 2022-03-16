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

package org.apache.seatunnel.flink;

/**
 * Jdbc source {@link org.apache.seatunnel.flink.source.JdbcSource} and
 * sink {@link org.apache.seatunnel.flink.sink.JdbcSink} configuration parameters
 */
public interface Config {

    /** Parallelism of the source or sink */
    String PARALLELISM = "parallelism";

    /** Jdbc driver for source or sink */
    String DRIVER = "driver";

    /** Jdbc Url for source or sink */
    String URL = "url";

    /** Jdbc username for source or sink */
    String USERNAME = "username";

    /** Jdbc query for source or sink */
    String QUERY = "query";

    /** Jdbc password for source or sink */
    String PASSWORD = "password";

    /** Jdbc fetch size for source */
    String SOURCE_FETCH_SIZE = "fetch_size";

    /** Jdbc batch size for sink */
    String SINK_BATCH_SIZE = "batch_size";

    /** Jdbc batch interval for sink */
    String SINK_BATCH_INTERVAL = "batch_interval";

    /** Jdbc max batch retries for sink */
    String SINK_BATCH_MAX_RETRIES = "batch_max_retries";

}

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

package org.apache.seatunnel.connectors.seatunnel.iotdb.config;

/**
 * SourceConfig is the configuration for the IotDBSource.
 * <p>
 * please see the following link for more details:
 * https://iotdb.apache.org/UserGuide/Master/API/Programming-Java-Native-API.html
 */
public class SourceConfig {

    public static final String SQL = "sql";

    /*---------------------- single node configurations -------------------------*/

    /**
     * The host of the IotDB server.
     */
    public static final String HOST = "host";

    /*
     * The port of the IotDB server.
     */
    public static final String PORT = "port";


    /*---------------------- multiple node configurations -------------------------*/

    /**
     * multiple nodes
     */
    public static final String NODE_URLS = "node_urls";

    /**
     * Query fields
     * e.g.
     * "name:TEXT,age:INT32,height:INT32"
     */
    public static final String FIELDS = "fields";

    /*---------------------- other configurations -------------------------*/

    /**
     * Fetches the next batch of data from the source.
     */
    public static final String FETCH_SIZE = "fetch_size";

    /**
     * Username for the source.
     */
    public static final String USERNAME = "username";

    /**
     * Password for the source.
     */
    public static final String PASSWORD = "password";

    /**
     * thrift default buffer size
     */
    public static final String THRIFT_DEFAULT_BUFFER_SIZE = "thrift_default_buffer_size";

    /**
     * thrift max frame size
     */
    public static final String THRIFT_MAX_FRAME_SIZE = "thrift_max_frame_size";

    /**
     * cassandra default buffer size
     */
    public static final String ENABLE_CACHE_LEADER = "enable_cache_leader";

    /**
     * Version represents the SQL semantic version used by the client, which is used to be compatible with the SQL semantics of 0.12 when upgrading 0.13. The possible values are: V_0_12, V_0_13.
     */
    public static final String VERSION = "version";

    /**
     * Query lower bound of the time range to be read.
     */
    public static final String LOWER_BOUND = "lower_bound";

    /**
     * Query upper bound of the time range to be read.
     */
    public static final String UPPER_BOUND = "upper_bound";

    /**
     * Query num partitions to be read.
     */
    public static final String NUM_PARTITIONS = "num_partitions";

}

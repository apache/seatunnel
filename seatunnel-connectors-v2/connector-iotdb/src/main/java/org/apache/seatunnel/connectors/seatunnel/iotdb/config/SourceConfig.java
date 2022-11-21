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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/**
 * SourceConfig is the configuration for the IotDBSource.
 * <p>
 * please see the following link for more details:
 * https://iotdb.apache.org/UserGuide/Master/API/Programming-Java-Native-API.html
 */
public class SourceConfig {

    public static final Option<String> SQL = Options.key("sql").stringType().noDefaultValue().withDescription("sql");

    /*---------------------- single node configurations -------------------------*/

    /**
     * The host of the IotDB server.
     */
    public static final Option<String> HOST = Options.key("host").stringType().noDefaultValue().withDescription("host");

    /*
     * The port of the IotDB server.
     */
    public static final Option<Integer> PORT = Options.key("port").intType().noDefaultValue().withDescription("port");


    /*---------------------- multiple node configurations -------------------------*/

    /**
     * Username for the source.
     */
    public static final Option<String> USERNAME = Options.key("username").stringType().noDefaultValue().withDescription("usernam");

    /**
     * Password for the source.
     */
    public static final Option<String> PASSWORD = Options.key("password").stringType().noDefaultValue().withDescription("password");

    /**
     * multiple nodes
     */
    public static final Option<String> NODE_URLS = Options.key("node_urls").stringType().noDefaultValue().withDescription("node urls");

    /*---------------------- other configurations -------------------------*/

    /**
     * Fetches the next batch of data from the source.
     */
    public static final Option<Integer> FETCH_SIZE = Options.key("fetch_size").intType().noDefaultValue().withDescription("fetch size");

    /**
     * thrift default buffer size
     */
    public static final Option<Integer> THRIFT_DEFAULT_BUFFER_SIZE = Options.key("thrift_default_buffer_size").intType().noDefaultValue().withDescription(" default thrift buffer size of iot db ");

    /**
     * thrift max frame size
     */
    public static final Option<Integer> THRIFT_MAX_FRAME_SIZE = Options.key("thrift_max_frame_size").intType().noDefaultValue().withDescription("thrift max frame size ");

    /**
     * cassandra default buffer size
     */
    public static final Option<Boolean> ENABLE_CACHE_LEADER = Options.key("enable_cache_leader").booleanType().noDefaultValue().withDescription("enable cache leader ");

    /**
     * Version represents the SQL semantic version used by the client, which is used to be compatible with the SQL semantics of 0.12 when upgrading 0.13. The possible values are: V_0_12, V_0_13.
     */
    public static final Option<String> VERSION = Options.key("version").stringType().noDefaultValue().withDescription("version");

    /**
     * Query lower bound of the time range to be read.
     */
    public static final Option<Long> LOWER_BOUND = Options.key("lower_bound").longType().noDefaultValue().withDescription("low bound");

    /**
     * Query upper bound of the time range to be read.
     */
    public static final Option<Long> UPPER_BOUND = Options.key("upper_bound").longType().noDefaultValue().withDescription("upper bound");

    /**
     * Query num partitions to be read.
     */
    public static final Option<Integer> NUM_PARTITIONS = Options.key("num_partitions").intType().noDefaultValue().withDescription("num partitions");

}

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

package org.apache.seatunnel.connectors.seatunnel.iotdbv2.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/**
 * SourceConfig is the configuration for the IotDBSource.
 *
 * <p>please see the following link for more details:
 * https://iotdb.apache.org/UserGuide/Master/API/Programming-Java-Native-API.html
 */
public class IoTDBv2SourceOptions extends IoTDBv2CommonOptions {

    /** Sql query */
    public static final Option<String> SQL =
            Options.key("sql").stringType().noDefaultValue().withDescription("sql");

    /** Database (only valid when sql_dialect is table) */
    public static final Option<String> DATABASE =
            Options.key("database").stringType().noDefaultValue().withDescription("database");

    /** Fetches the next batch of data from the source. */
    public static final Option<Integer> FETCH_SIZE =
            Options.key("fetch_size").intType().noDefaultValue().withDescription("fetch size");

    /** thrift default buffer size */
    public static final Option<Integer> DEFAULT_THRIFT_BUFFER_SIZE =
            Options.key("default_thrift_buffer_size")
                    .intType()
                    .noDefaultValue()
                    .withDescription(" default thrift buffer size");

    /** thrift max frame size */
    public static final Option<Integer> MAX_THRIFT_FRAME_SIZE =
            Options.key("max_thrift_frame_size")
                    .intType()
                    .noDefaultValue()
                    .withDescription("max thrift frame size ");

    /** cassandra default buffer size */
    public static final Option<Boolean> ENABLE_CACHE_LEADER =
            Options.key("enable_cache_leader")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("enable cache leader ");

    /** Query lower bound of the time range to be read. */
    public static final Option<Long> LOWER_BOUND =
            Options.key("lower_bound").longType().noDefaultValue().withDescription("lower bound");

    /** Query upper bound of the time range to be read. */
    public static final Option<Long> UPPER_BOUND =
            Options.key("upper_bound").longType().noDefaultValue().withDescription("upper bound");

    /** Query num partitions to be read. */
    public static final Option<Integer> NUM_PARTITIONS =
            Options.key("num_partitions")
                    .intType()
                    .noDefaultValue()
                    .withDescription("num partitions");
}

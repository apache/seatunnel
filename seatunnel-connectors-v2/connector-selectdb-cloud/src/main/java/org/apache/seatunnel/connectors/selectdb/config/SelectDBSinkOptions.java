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

package org.apache.seatunnel.connectors.selectdb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Map;
import java.util.UUID;

public class SelectDBSinkOptions {

    public static final String IDENTIFIER = "SelectDBCloud";

    private static final int DEFAULT_SINK_MAX_RETRIES = 3;
    private static final int DEFAULT_SINK_BUFFER_SIZE = 10 * 1024 * 1024;
    private static final int DEFAULT_SINK_BUFFER_COUNT = 10000;
    // common option
    public static final Option<String> LOAD_URL =
            Options.key("load-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SelectDB load http address.");

    public static final Option<String> JDBC_URL =
            Options.key("jdbc-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SelectDB jdbc query address.");

    public static final Option<String> CLUSTER_NAME =
            Options.key("cluster-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SelectDB cluster name.");

    public static final Option<String> TABLE_IDENTIFIER =
            Options.key("table.identifier")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the jdbc table name.");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the jdbc user name.");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the jdbc password.");

    public static final Option<Boolean> SINK_ENABLE_2PC =
            Options.key("sink.enable-2pc")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("enable 2PC while loading");
    // sink config options
    public static final Option<Integer> SINK_MAX_RETRIES =
            Options.key("sink.max-retries")
                    .intType()
                    .defaultValue(DEFAULT_SINK_MAX_RETRIES)
                    .withDescription("the max retry times if writing records to database failed.");

    public static final Option<Integer> SINK_BUFFER_SIZE =
            Options.key("sink.buffer-size")
                    .intType()
                    .defaultValue(DEFAULT_SINK_BUFFER_SIZE)
                    .withDescription("the buffer size to cache data for stream load.");

    public static final Option<Integer> SINK_BUFFER_COUNT =
            Options.key("sink.buffer-count")
                    .intType()
                    .defaultValue(DEFAULT_SINK_BUFFER_COUNT)
                    .withDescription("the buffer count to cache data for stream load.");

    public static final Option<String> SINK_LABEL_PREFIX =
            Options.key("sink.label-prefix")
                    .stringType()
                    .defaultValue(UUID.randomUUID().toString())
                    .withDescription("the unique label prefix.");

    public static final Option<Boolean> SINK_ENABLE_DELETE =
            Options.key("sink.enable-delete")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("whether to enable the delete function");

    public static final Option<Integer> SINK_FLUSH_QUEUE_SIZE =
            Options.key("sink.flush.queue-size")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Queue length for async upload to object storage");

    public static final Option<Map<String, String>> SELECTDB_SINK_CONFIG_PREFIX =
            Options.key("selectdb.config")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "The parameter of the Copy Into data_desc. "
                                    + "The way to specify the parameter is to add the prefix `selectdb.config` to the original load parameter name ");
}

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

package org.apache.seatunnel.connectors.doris.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public class DorisSourceOptions extends DorisBaseOptions {

    public static final String DORIS_DEFAULT_CLUSTER = "default_cluster";
    public static final int DORIS_TABLET_SIZE_MIN = 1;
    public static final int DORIS_TABLET_SIZE_DEFAULT = Integer.MAX_VALUE;
    public static final int DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT = 30 * 1000;
    public static final int DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT = 30 * 1000;
    public static final int DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT = 3600;
    public static final int DORIS_REQUEST_RETRIES_DEFAULT = 3;
    public static final Boolean DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT = false;
    public static final int DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT = 64;
    public static final long DORIS_EXEC_MEM_LIMIT_DEFAULT = 2147483648L;

    public static final Option<List<DorisTableConfig>> TABLE_LIST =
            Options.key("table_list")
                    .listType(DorisTableConfig.class)
                    .noDefaultValue()
                    .withDescription("table list config.");

    public static final Option<String> DORIS_READ_FIELD =
            Options.key("doris.read.field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "List of column names in the Doris table, separated by commas");
    public static final Option<String> DORIS_FILTER_QUERY =
            Options.key("doris.filter.query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Filter expression of the query, which is transparently transmitted to Doris. Doris uses this expression to complete source-side data filtering");

    public static final Option<Integer> DORIS_TABLET_SIZE =
            Options.key("doris.request.tablet.size")
                    .intType()
                    .defaultValue(DORIS_TABLET_SIZE_DEFAULT)
                    .withDescription("");

    public static final Option<Integer> DORIS_REQUEST_CONNECT_TIMEOUT_MS =
            Options.key("doris.request.connect.timeout.ms")
                    .intType()
                    .defaultValue(DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT)
                    .withDescription("");

    public static final Option<Integer> DORIS_REQUEST_READ_TIMEOUT_MS =
            Options.key("doris.request.read.timeout.ms")
                    .intType()
                    .defaultValue(DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT)
                    .withDescription("");

    public static final Option<Integer> DORIS_REQUEST_QUERY_TIMEOUT_S =
            Options.key("doris.request.query.timeout.s")
                    .intType()
                    .defaultValue(DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT)
                    .withDescription("");

    public static final Option<Integer> DORIS_REQUEST_RETRIES =
            Options.key("doris.request.retries")
                    .intType()
                    .defaultValue(DORIS_REQUEST_RETRIES_DEFAULT)
                    .withDescription("");

    public static final Option<Boolean> DORIS_DESERIALIZE_ARROW_ASYNC =
            Options.key("doris.deserialize.arrow.async")
                    .booleanType()
                    .defaultValue(DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT)
                    .withDescription("");

    public static final Option<Integer> DORIS_DESERIALIZE_QUEUE_SIZE =
            Options.key("doris.request.retriesdoris.deserialize.queue.size")
                    .intType()
                    .defaultValue(DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT)
                    .withDescription("");

    public static final Option<Long> DORIS_EXEC_MEM_LIMIT =
            Options.key("doris.exec.mem.limit")
                    .longType()
                    .defaultValue(DORIS_EXEC_MEM_LIMIT_DEFAULT)
                    .withDescription("");
}

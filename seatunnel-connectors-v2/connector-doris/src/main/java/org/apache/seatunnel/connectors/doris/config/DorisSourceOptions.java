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

public interface DorisSourceOptions {

    int DORIS_TABLET_SIZE_MIN = 1;
    int DORIS_TABLET_SIZE_DEFAULT = Integer.MAX_VALUE;
    int DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT = 30 * 1000;
    int DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT = 30 * 1000;
    int DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT = 3600;
    int DORIS_REQUEST_RETRIES_DEFAULT = 3;
    Boolean DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT = false;
    int DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT = 64;
    long DORIS_EXEC_MEM_LIMIT_DEFAULT = 2147483648L;

    Option<List<DorisTableConfig>> TABLE_LIST =
            Options.key("table_list")
                    .listType(DorisTableConfig.class)
                    .noDefaultValue()
                    .withDescription("table list config.");

    Option<String> DORIS_READ_FIELD =
            Options.key("doris.read.field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "List of column names in the Doris table, separated by commas");
    Option<String> DORIS_FILTER_QUERY =
            Options.key("doris.filter.query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Filter expression of the query, which is transparently transmitted to Doris. Doris uses this expression to complete source-side data filtering");

    Option<Integer> DORIS_TABLET_SIZE =
            Options.key("doris.request.tablet.size")
                    .intType()
                    .defaultValue(DORIS_TABLET_SIZE_DEFAULT)
                    .withDescription("");

    Option<Integer> DORIS_REQUEST_CONNECT_TIMEOUT_MS =
            Options.key("doris.request.connect.timeout.ms")
                    .intType()
                    .defaultValue(DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT)
                    .withDescription("");

    Option<Integer> DORIS_REQUEST_READ_TIMEOUT_MS =
            Options.key("doris.request.read.timeout.ms")
                    .intType()
                    .defaultValue(DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT)
                    .withDescription("");

    Option<Integer> DORIS_REQUEST_QUERY_TIMEOUT_S =
            Options.key("doris.request.query.timeout.s")
                    .intType()
                    .defaultValue(DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT)
                    .withDescription("");

    Option<Integer> DORIS_REQUEST_RETRIES =
            Options.key("doris.request.retries")
                    .intType()
                    .defaultValue(DORIS_REQUEST_RETRIES_DEFAULT)
                    .withDescription("");

    Option<Boolean> DORIS_DESERIALIZE_ARROW_ASYNC =
            Options.key("doris.deserialize.arrow.async")
                    .booleanType()
                    .defaultValue(DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT)
                    .withDescription("");

    Option<Integer> DORIS_DESERIALIZE_QUEUE_SIZE =
            Options.key("doris.request.retriesdoris.deserialize.queue.size")
                    .intType()
                    .defaultValue(DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT)
                    .withDescription("");

    Option<Long> DORIS_EXEC_MEM_LIMIT =
            Options.key("doris.exec.mem.limit")
                    .longType()
                    .defaultValue(DORIS_EXEC_MEM_LIMIT_DEFAULT)
                    .withDescription("");
}

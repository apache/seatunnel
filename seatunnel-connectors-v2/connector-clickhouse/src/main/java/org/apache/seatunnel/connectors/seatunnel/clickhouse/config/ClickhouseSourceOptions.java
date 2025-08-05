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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public class ClickhouseSourceOptions {

    public static final int CLICKHOUSE_SPLIT_SIZE_MIN = 1;
    public static final int CLICKHOUSE_SPLIT_SIZE_DEFAULT = Integer.MAX_VALUE;
    public static final int CLICKHOUSE_BATCH_SIZE_DEFAULT = 1024;

    public static final Option<Integer> CLICKHOUSE_SPLIT_SIZE =
            Options.key("split.size")
                    .intType()
                    .defaultValue(CLICKHOUSE_SPLIT_SIZE_DEFAULT)
                    .withDescription("The number of parts in each splits");

    public static final Option<List<String>> CLICKHOUSE_PARTITION_LIST =
            Options.key("partition_list")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "The partition used to filter data, if not set, the whole table will be queried");

    public static final Option<Integer> CLICKHOUSE_BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(CLICKHOUSE_BATCH_SIZE_DEFAULT)
                    .withDescription(
                            "The maximum rows of data that can be obtained by reading from Clickhouse once.");

    public static final Option<String> SQL =
            Options.key("sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Clickhouse sql used to query data");

    public static final Option<String> CLICKHOUSE_FILTER_QUERY =
            Options.key("filter_query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Filter expression of the query. such as id > 2.");
}

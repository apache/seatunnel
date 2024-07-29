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

package org.apache.seatunnel.connectors.seatunnel.jdbc.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

@SuppressWarnings("checkstyle:MagicNumber")
public interface JdbcSourceOptions {

    Option<String> TABLE_PATH =
            Options.key("table_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("table full path");

    Option<String> WHERE_CONDITION =
            Options.key("where_condition")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Common row filter conditions for all tables/queries, must start with `where`. for example `where id > 100`");

    Option<List<JdbcSourceTableConfig>> TABLE_LIST =
            Options.key("table_list")
                    .listType(JdbcSourceTableConfig.class)
                    .noDefaultValue()
                    .withDescription("table list config");

    Option<Integer> SPLIT_SIZE =
            Options.key("split.size")
                    .intType()
                    .defaultValue(8096)
                    .withDescription(
                            "The split size (number of rows) of table snapshot, captured tables are split into multiple splits when read     of table.");

    Option<Double> SPLIT_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND =
            Options.key("split.even-distribution.factor.upper-bound")
                    .doubleType()
                    .defaultValue(100.0d)
                    .withDescription(
                            "The upper bound of split key distribution factor. The distribution factor is used to determine whether the"
                                    + " table is evenly distribution or not."
                                    + " The table chunks would use evenly calculation optimization when the data distribution is even,"
                                    + " and the query for splitting would happen when it is uneven."
                                    + " The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.");

    Option<Double> SPLIT_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND =
            Options.key("split.even-distribution.factor.lower-bound")
                    .doubleType()
                    .defaultValue(0.05d)
                    .withDescription(
                            "The lower bound of split key distribution factor. The distribution factor is used to determine whether the"
                                    + " table is evenly distribution or not."
                                    + " The table chunks would use evenly calculation optimization when the data distribution is even,"
                                    + " and the query for splitting would happen when it is uneven."
                                    + " The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.");

    Option<Integer> SPLIT_SAMPLE_SHARDING_THRESHOLD =
            Options.key("split.sample-sharding.threshold")
                    .intType()
                    .defaultValue(1000) // 1000 shards
                    .withDescription(
                            "The threshold of estimated shard count to trigger the sample sharding strategy. "
                                    + "When the distribution factor is outside the upper and lower bounds, "
                                    + "and if the estimated shard count (approximateRowCnt/chunkSize) exceeds this threshold, "
                                    + "the sample sharding strategy will be used. "
                                    + "This strategy can help to handle large datasets more efficiently. "
                                    + "The default value is 1000 shards.");
    Option<Integer> SPLIT_INVERSE_SAMPLING_RATE =
            Options.key("split.inverse-sampling.rate")
                    .intType()
                    .defaultValue(1000) // 1/1000 sampling rate
                    .withDescription(
                            "The inverse of the sampling rate for the sample sharding strategy. "
                                    + "The value represents the denominator of the sampling rate fraction. "
                                    + "For example, a value of 1000 means a sampling rate of 1/1000. "
                                    + "This parameter is used when the sample sharding strategy is triggered.");

    Option<Boolean> USE_SELECT_COUNT =
            Options.key("use_select_count")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Use select count for table count");

    Option<Boolean> SKIP_ANALYZE =
            Options.key("skip_analyze")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Skip the analysis of table count");
}

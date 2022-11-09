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

package org.apache.seatunnel.connectors.seatunnel.cdc.base.options;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.IncrementalSource;

/** Configurations for {@link IncrementalSource}. */
@SuppressWarnings("checkstyle:MagicNumber")
public class SourceOptions {

    public static final Option<Boolean> SCAN_INCREMENTAL_SNAPSHOT_ENABLED =
            Options.key("scan.incremental.snapshot.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Incremental snapshot is a new mechanism to read snapshot of a table. "
                                    + "Compared to the old snapshot mechanism, the incremental snapshot has many advantages, including:\n"
                                    + "(1) source can be parallel during snapshot reading, \n"
                                    + "(2) source can perform checkpoints in the chunk granularity during snapshot reading, \n"
                                    + "(3) source doesn't need to acquire global read lock (FLUSH TABLES WITH READ LOCK) before snapshot reading.\n"
                                    + "If you would like the source run in parallel, each parallel reader should have an unique server id, "
                                    + "so the 'server-id' must be a range like '5400-6400', and the range must be larger than the parallelism.");

    public static final Option<Integer> SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE =
            Options.key("scan.incremental.snapshot.chunk.size")
                    .intType()
                    .defaultValue(8096)
                    .withDescription(
                            "The chunk size (number of rows) of table snapshot, captured tables are split into multiple chunks when read the snapshot of table.");

    public static final Option<Integer> SCAN_SNAPSHOT_FETCH_SIZE =
            Options.key("scan.snapshot.fetch.size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "The maximum fetch size for per poll when read table snapshot.");

    public static final Option<String> SCAN_STARTUP_MODE =
            Options.key("scan.startup.mode")
                    .stringType()
                    .defaultValue("initial")
                    .withDescription(
                            "Optional startup mode for CDC consumer, valid enumerations are "
                                    + "\"initial\", \"earliest-offset\", \"latest-offset\", \"timestamp\"\n"
                                    + "or \"specific-offset\"");

    public static final Option<String> SCAN_STARTUP_SPECIFIC_OFFSET_FILE =
            Options.key("scan.startup.specific-offset.file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional offsets used in case of \"specific-offset\" startup mode");

    public static final Option<Integer> SCAN_STARTUP_SPECIFIC_OFFSET_POS =
            Options.key("scan.startup.specific-offset.pos")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional offsets used in case of \"specific-offset\" startup mode");

    public static final Option<Long> SCAN_STARTUP_TIMESTAMP_MILLIS =
            Options.key("scan.startup.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp used in case of \"timestamp\" startup mode");

    public static final Option<Integer> CHUNK_META_GROUP_SIZE =
            Options.key("chunk-meta.group.size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The group size of chunk meta, if the meta size exceeds the group size, the meta will be divided into multiple groups.");

    public static final Option<Double> SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND =
            Options.key("chunk-key.even-distribution.factor.upper-bound")
                    .doubleType()
                    .defaultValue(1000.0d)
                    .withDescription(
                            "The upper bound of chunk key distribution factor. The distribution factor is used to determine whether the"
                                    + " table is evenly distribution or not."
                                    + " The table chunks would use evenly calculation optimization when the data distribution is even,"
                                    + " and the query for splitting would happen when it is uneven."
                                    + " The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.");

    public static final Option<Double> SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND =
            Options.key("chunk-key.even-distribution.factor.lower-bound")
                    .doubleType()
                    .defaultValue(0.05d)
                    .withDescription(
                            "The lower bound of chunk key distribution factor. The distribution factor is used to determine whether the"
                                    + " table is evenly distribution or not."
                                    + " The table chunks would use evenly calculation optimization when the data distribution is even,"
                                    + " and the query for splitting would happen when it is uneven."
                                    + " The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.");
}

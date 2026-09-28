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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.config;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Map;

public class HugeGraphSourceOptions {

    public static final int MIN_PAGE_SIZE = 100;
    public static final int MAX_PAGE_SIZE = 10000;
    // Lower bound for split_size (1 MiB), matching the HugeGraph server's own minimum shard size.
    // A smaller value shatters the keyspace into a huge number of shards — one split per shard,
    // each
    // persisted into every checkpoint — risking OOM / oversized checkpoints, and the server rejects
    // it anyway; reject it up front with a clear message.
    public static final long MIN_SPLIT_SIZE = 1048576L;

    public static final Option<String> LABEL =
            Options.key("label")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("HugeGraph vertex label or edge label to read");

    public static final Option<MappingConfig.LabelType> LABEL_TYPE =
            Options.key("label_type")
                    .enumType(MappingConfig.LabelType.class)
                    .defaultValue(MappingConfig.LabelType.VERTEX)
                    .withDescription("HugeGraph label type. Supported values are VERTEX and EDGE");

    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Records per HugeGraph page, must be in range [100, 10000]");

    public static final Option<String> TIME_ZONE =
            Options.key("time_zone")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Time zone used to convert HugeGraph DATE values that the server returns "
                                    + "as an epoch/Date (the instant is rendered as a local date-time "
                                    + "in this zone). It does NOT apply when the server returns a DATE "
                                    + "already serialized as a wall-clock string (e.g. "
                                    + "'yyyy-MM-dd HH:mm:ss.SSS') — that value is kept verbatim, since "
                                    + "its original zone is not carried in the string. When omitted, "
                                    + "the worker JVM default time zone is used for backward "
                                    + "compatibility.");

    public static final Option<Long> SPLIT_SIZE =
            Options.key("split_size")
                    .longType()
                    .defaultValue(1048576L)
                    .withDescription(
                            "Target size in bytes of each key-range shard when parallelism > 1. "
                                    + "The server splits the keyspace into shards of roughly this "
                                    + "size and readers scan them in parallel; a larger value yields "
                                    + "fewer, bigger shards. Ignored when parallelism = 1 (which uses "
                                    + "the single label-list scan). Requires a scan-capable backend "
                                    + "(RocksDB / HBase / Cassandra).");

    public static final Option<Map<String, Object>> FILTER =
            Options.key("filter")
                    .type(new TypeReference<Map<String, Object>>() {})
                    .noDefaultValue()
                    .withDescription(
                            "Optional property equality conditions applied server-side when "
                                    + "reading the label, e.g. { country = \"US\", active = \"true\" }. "
                                    + "Only elements whose properties match all entries are returned. "
                                    + "Every key must be a property of the configured label. When "
                                    + "omitted, all elements of the label are read.");
}

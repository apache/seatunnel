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

package org.apache.seatunnel.connectors.cdc.base.option;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.connectors.cdc.debezium.DeserializeFormat;

import java.util.Map;

@SuppressWarnings("MagicNumber")
public class SourceOptions {

    public static final String STARTUP_MODE_KEY = "startup.mode";
    public static final String STOP_MODE_KEY = "stop.mode";

    public static final Option<Integer> SNAPSHOT_SPLIT_SIZE =
            Options.key("snapshot.split.size")
                    .intType()
                    .defaultValue(8096)
                    .withDescription(
                            "The split size (number of rows) of table snapshot, captured tables are split into multiple splits when read the snapshot of table.");

    public static final Option<Integer> SNAPSHOT_FETCH_SIZE =
            Options.key("snapshot.fetch.size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "The maximum fetch size for per poll when read table snapshot.");

    public static final Option<Long> STARTUP_TIMESTAMP =
            Options.key("startup.timestamp")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp(mills) used in case of \"timestamp\" startup mode");

    public static final Option<String> STARTUP_SPECIFIC_OFFSET_FILE =
            Options.key("startup.specific-offset.file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional offsets used in case of \"specific\" startup mode");

    public static final Option<Long> STARTUP_SPECIFIC_OFFSET_POS =
            Options.key("startup.specific-offset.pos")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Optional offsets used in case of \"specific\" startup mode");

    public static final Option<Integer> INCREMENTAL_PARALLELISM =
            Options.key("incremental.parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription("The number of parallel readers in the incremental phase.");

    public static final Option<Long> STOP_TIMESTAMP =
            Options.key("stop.timestamp")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp(mills) used in case of \"timestamp\" stop mode");

    public static final Option<String> STOP_SPECIFIC_OFFSET_FILE =
            Options.key("stop.specific-offset.file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional offsets used in case of \"specific\" stop mode");

    public static final Option<Long> STOP_SPECIFIC_OFFSET_POS =
            Options.key("stop.specific-offset.pos")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Optional offsets used in case of \"specific\" stop mode");

    public static final Option<Map<String, String>> DEBEZIUM_PROPERTIES =
            Options.key("debezium")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Decides if the table options contains Debezium client properties that start with prefix 'debezium'.");

    public static final Option<DeserializeFormat> FORMAT =
            Options.key("format")
                    .enumType(DeserializeFormat.class)
                    .defaultValue(DeserializeFormat.DEFAULT)
                    .withDescription(
                            "Data format. The default format is seatunnel row. Optional compatible with debezium-json format.");

    public static final Option<Boolean> EXACTLY_ONCE =
            Options.key("exactly_once")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Enable exactly once semantic.");

    public static OptionRule.Builder getBaseRule() {
        return OptionRule.builder()
                .optional(FORMAT)
                .optional(SNAPSHOT_SPLIT_SIZE, SNAPSHOT_FETCH_SIZE)
                .optional(INCREMENTAL_PARALLELISM)
                .optional(DEBEZIUM_PROPERTIES);
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.kafka.config;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Map;

public class KafkaSourceOptions extends KafkaBaseOptions {

    public static final Option<Boolean> PATTERN =
            Options.key("pattern")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If pattern is set to true,the regular expression for a pattern of topic names to read from."
                                    + " All topics in clients with names that match the specified regular expression will be subscribed by the consumer.");

    public static final Option<String> CONSUMER_GROUP =
            Options.key("consumer.group")
                    .stringType()
                    .defaultValue("SeaTunnel-Consumer-Group")
                    .withDescription(
                            "Kafka consumer group id, used to distinguish different consumer groups.");

    public static final Option<Boolean> COMMIT_ON_CHECKPOINT =
            Options.key("commit_on_checkpoint")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "If true the consumer's offset will be periodically committed in the background.");

    public static final Option<Boolean> DEBEZIUM_RECORD_INCLUDE_SCHEMA =
            Options.key("debezium_record_include_schema")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Does the debezium record carry a schema.");

    public static final Option<TableIdentifierConfig> DEBEZIUM_RECORD_TABLE_FILTER =
            Options.key("debezium_record_table_filter")
                    .type(new TypeReference<TableIdentifierConfig>() {})
                    .noDefaultValue()
                    .withDescription("Debezium record table filter.");

    public static final Option<StartMode> START_MODE =
            Options.key("start_mode")
                    .objectType(StartMode.class)
                    .defaultValue(StartMode.GROUP_OFFSETS)
                    .withDescription(
                            "The initial consumption pattern of consumers,there are several types:\n"
                                    + "[earliest],[group_offsets],[latest],[specific_offsets],[timestamp]");

    public static final Option<Long> START_MODE_TIMESTAMP =
            Options.key("start_mode.timestamp")
                    .longType()
                    .noDefaultValue()
                    .withDescription("The time required for consumption mode to be timestamp.");

    public static final Option<Map<String, Long>> START_MODE_OFFSETS =
            Options.key("start_mode.offsets")
                    .type(new TypeReference<Map<String, Long>>() {})
                    .noDefaultValue()
                    .withDescription(
                            "The offset required for consumption mode to be specific_offsets.");

    /** Configuration key to define the consumer's partition discovery interval, in milliseconds. */
    public static final Option<Long> KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS =
            Options.key("partition-discovery.interval-millis")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription(
                            "The interval for dynamically discovering topics and partitions.");

    public static final Option<Long> KEY_POLL_TIMEOUT =
            Options.key("poll.timeout")
                    .longType()
                    .defaultValue(10000L)
                    .withDescription("The interval for poll message");

    public static final Option<MessageFormatErrorHandleWay> MESSAGE_FORMAT_ERROR_HANDLE_WAY_OPTION =
            Options.key("format_error_handle_way")
                    .enumType(MessageFormatErrorHandleWay.class)
                    .defaultValue(MessageFormatErrorHandleWay.FAIL)
                    .withDescription(
                            "The processing method of data format error. The default value is fail, and the optional value is (fail, skip). "
                                    + "When fail is selected, data format error will block and an exception will be thrown. "
                                    + "When skip is selected, data format error will skip this line data.");
}

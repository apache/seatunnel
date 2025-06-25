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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.config;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.StartMode;

import java.util.Map;

public class RocketMqSourceOptions extends RocketMqBaseOptions {

    private static final String DEFAULT_CONSUMER_GROUP = "SeaTunnel-Consumer-Group";
    private static final long DEFAULT_POLL_TIMEOUT_MILLIS = 5000;
    private static final int DEFAULT_BATCH_SIZE = 100;

    public static final Option<String> TOPICS =
            Options.key("topics")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "RocketMq topic name. If there are multiple topics, use `,` to split, for example: "
                                    + "\"tpc1,tpc2\".");

    public static final Option<String> TAGS =
            Options.key("tags")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "RocketMq tag name. If there are multiple tags, use `,` to split, for example: "
                                    + "\"tag1,tag2\".");

    public static final Option<StartMode> START_MODE =
            Options.key("start.mode")
                    .objectType(StartMode.class)
                    .defaultValue(StartMode.CONSUME_FROM_GROUP_OFFSETS)
                    .withDescription(
                            "The initial consumption pattern of consumers,there are several types:\n"
                                    + "[CONSUME_FROM_LAST_OFFSET],[CONSUME_FROM_FIRST_OFFSET],[CONSUME_FROM_GROUP_OFFSETS],[CONSUME_FROM_TIMESTAMP],[CONSUME_FROM_SPECIFIC_OFFSETS]");

    public static final Option<Long> START_MODE_TIMESTAMP =
            Options.key("start.mode.timestamp")
                    .longType()
                    .noDefaultValue()
                    .withDescription("The time required for consumption mode to be timestamp.");

    public static final Option<Map<String, Long>> START_MODE_OFFSETS =
            Options.key("start.mode.offsets")
                    .type(new TypeReference<Map<String, Long>>() {})
                    .noDefaultValue()
                    .withDescription(
                            "The offset required for consumption mode to be specific offsets.");

    /** Configuration key to define the consumer's partition discovery interval, in milliseconds. */
    public static final Option<Long> KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS =
            Options.key("partition.discovery" + ".interval.millis")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription(
                            "The interval for dynamically discovering topics and partitions.");

    public static final Option<String> CONSUMER_GROUP =
            Options.key("consumer.group")
                    .stringType()
                    .defaultValue(DEFAULT_CONSUMER_GROUP)
                    .withDescription("RocketMq consumer group id.");

    public static final Option<Boolean> COMMIT_ON_CHECKPOINT =
            Options.key("commit.on.checkpoint")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "If true, the consumer's offset will be stored in the background periodically.");

    public static final Option<Long> POLL_TIMEOUT_MILLIS =
            Options.key("consumer.poll.timeout.millis")
                    .longType()
                    .defaultValue(DEFAULT_POLL_TIMEOUT_MILLIS)
                    .withDescription("The poll timeout in milliseconds.");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch.size")
                    .intType()
                    .defaultValue(DEFAULT_BATCH_SIZE)
                    .withDescription("Rocketmq consumer pull batch size.");

    public static final Option<Boolean> IGNORE_PARSE_ERRORS =
            Options.key("ignore_parse_errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Optional flag to skip parse errors instead of failing.");
}

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

package org.apache.seatunnel.connectors.seatunnel.pulsar.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class PulsarSourceOptions extends PulsarBaseOptions {

    private static final Long DEFAULT_TOPIC_DISCOVERY_INTERVAL = -1L;
    private static final Integer DEFAULT_POLL_TIMEOUT = 100;
    private static final Long DEFAULT_POLL_INTERVAL = 50L;
    private static final Integer DEFAULT_POLL_BATCH_SIZE = 500;

    public static final Option<String> SUBSCRIPTION_NAME =
            Options.key("subscription.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specify the subscription name for this consumer. This argument is required when constructing the consumer.");

    public static final Option<String> TOPIC_PATTERN =
            Options.key("topic-pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The regular expression for a pattern of topic names to read from. All topics with names that match the specified regular expression will be subscribed by the consumer when the job starts running. Note, only one of \"topic-pattern\" and \"topic\" can be specified for sources.");

    public static final Option<Integer> POLL_TIMEOUT =
            Options.key("poll.timeout")
                    .intType()
                    .defaultValue(DEFAULT_POLL_TIMEOUT)
                    .withDescription(
                            "Default value is "
                                    + DEFAULT_POLL_TIMEOUT
                                    + ". The maximum time (in ms) to wait when fetching records. A longer time increases throughput but also latency.");

    public static final Option<Long> POLL_INTERVAL =
            Options.key("poll.interval")
                    .longType()
                    .defaultValue(DEFAULT_POLL_INTERVAL)
                    .withDescription(
                            "Default value is "
                                    + DEFAULT_POLL_INTERVAL
                                    + ". The interval time(in ms) when fetcing records. A shorter time increases throughput, but also increases CPU load.");

    public static final Option<Integer> POLL_BATCH_SIZE =
            Options.key("poll.batch.size")
                    .intType()
                    .defaultValue(DEFAULT_POLL_BATCH_SIZE)
                    .withDescription(
                            "Default value is "
                                    + DEFAULT_POLL_BATCH_SIZE
                                    + ". The maximum number of records to fetch to wait when polling. A longer time increases throughput but also latency");

    public static final Option<StartMode> CURSOR_STARTUP_MODE =
            Options.key("cursor.startup.mode")
                    .enumType(StartMode.class)
                    .defaultValue(StartMode.LATEST)
                    .withDescription(
                            "Startup mode for Pulsar consumer, valid values are 'EARLIEST', 'LATEST', 'SUBSCRIPTION', 'TIMESTAMP'.");

    public static final Option<CursorResetStrategy> CURSOR_RESET_MODE =
            Options.key("cursor.reset.mode")
                    .enumType(CursorResetStrategy.class)
                    .noDefaultValue()
                    .withDescription(
                            "Cursor reset strategy for Pulsar consumer valid values are 'EARLIEST', 'LATEST'. Note, This option only works if the \"cursor.startup.mode\" option used 'SUBSCRIPTION'.");

    public static final Option<Long> CURSOR_STARTUP_TIMESTAMP =
            Options.key("cursor.startup.timestamp")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Start from the specified epoch timestamp (in milliseconds). Note, This option is required when the \"cursor.startup.mode\" option used 'TIMESTAMP'.");

    public static final Option<StopMode> CURSOR_STOP_MODE =
            Options.key("cursor.stop.mode")
                    .enumType(StopMode.class)
                    .defaultValue(StopMode.NEVER)
                    .withDescription(
                            "Stop mode for Pulsar consumer, valid values are 'NEVER', 'LATEST' and 'TIMESTAMP'. Note, When 'NEVER' is specified, it is a real-time job, and other mode are off-line jobs.");

    public static final Option<Long> CURSOR_STOP_TIMESTAMP =
            Options.key("cursor.stop.timestamp")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Stop from the specified epoch timestamp (in milliseconds)");

    public static final Option<Long> TOPIC_DISCOVERY_INTERVAL =
            Options.key("topic-discovery.interval")
                    .longType()
                    .defaultValue(DEFAULT_TOPIC_DISCOVERY_INTERVAL)
                    .withDescription(
                            "Default value is "
                                    + DEFAULT_TOPIC_DISCOVERY_INTERVAL
                                    + ". The interval (in ms) for the Pulsar source to discover the new topic partitions. A non-positive value disables the topic partition discovery. Note, This option only works if the 'topic-pattern' option is used.");

    /** Startup mode for the pulsar consumer, see {@link #CURSOR_STARTUP_MODE}. */
    public enum StartMode {
        /** Start from the earliest cursor possible. */
        EARLIEST,
        /** Start from the latest cursor. */
        LATEST,
        /** Start from committed cursors in a specific consumer subscription. */
        SUBSCRIPTION,
        /** Start from user-supplied timestamp for each partition. */
        TIMESTAMP,
        /** Start from user-supplied specific cursors for each partition. */
        SPECIFIC
    }

    /** Stop mode for the pulsar consumer, see {@link #CURSOR_STOP_MODE}. */
    public enum StopMode {
        /** Stop from the latest cursor. */
        LATEST,
        /** Stop from user-supplied timestamp for each partition. */
        TIMESTAMP,
        /** Stop from user-supplied specific cursors for each partition. */
        SPECIFIC,
        NEVER
    }

    public enum CursorResetStrategy {
        LATEST,
        EARLIEST
    }
}

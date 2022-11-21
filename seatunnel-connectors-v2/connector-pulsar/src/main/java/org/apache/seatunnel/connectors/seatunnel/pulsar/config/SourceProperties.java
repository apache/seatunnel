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

public class SourceProperties {

    private static final Long DEFAULT_TOPIC_DISCOVERY_INTERVAL = -1L;
    private static final Integer DEFAULT_POLL_TIMEOUT = 100;
    private static final Long DEFAULT_POLL_INTERVAL = 50L;
    private static final Integer DEFAULT_POLL_BATCH_SIZE = 500;

    // --------------------------------------------------------------------------------------------
    // The configuration for ClientConfigurationData part.
    // --------------------------------------------------------------------------------------------

    public static final Option<String> CLIENT_SERVICE_URL =
        Options.key("client.service-url")
            .stringType()
            .noDefaultValue()
            .withDescription(
                "Service URL provider for Pulsar service");

    public static final Option<String> AUTH_PLUGIN_CLASS =
        Options.key("auth.plugin-class")
            .stringType()
            .noDefaultValue()
            .withDescription("Name of the authentication plugin");

    public static final Option<String> AUTH_PARAMS =
        Options.key("auth.params")
            .stringType()
            .noDefaultValue()
            .withDescription("Parameters for the authentication plugin. For example, key1:val1,key2:val2");

    // --------------------------------------------------------------------------------------------
    // The configuration for ClientConfigurationData part.
    // All the configuration listed below should have the pulsar.client prefix.
    // --------------------------------------------------------------------------------------------

    public static final Option<String> ADMIN_SERVICE_URL =
        Options.key("admin.service-url")
            .stringType()
            .noDefaultValue()
            .withDescription(
                "The Pulsar service HTTP URL for the admin endpoint. For example, http://my-broker.example.com:8080, or https://my-broker.example.com:8443 for TLS.");

    // --------------------------------------------------------------------------------------------
    // The configuration for ConsumerConfigurationData part.
    // --------------------------------------------------------------------------------------------

    public static final Option<String> SUBSCRIPTION_NAME =
        Options.key("subscription.name")
            .stringType()
            .noDefaultValue()
            .withDescription(
                "Specify the subscription name for this consumer. This argument is required when constructing the consumer.");

    // No use parameter
    public static final String SUBSCRIPTION_TYPE = "subscription.type";
    public static final String SUBSCRIPTION_MODE = "subscription.mode";

    // --------------------------------------------------------------------------------------------
    // The configuration for pulsar source part.
    // --------------------------------------------------------------------------------------------

    public static final Option<Long> TOPIC_DISCOVERY_INTERVAL =
        Options.key("topic-discovery.interval")
            .longType()
            .defaultValue(DEFAULT_TOPIC_DISCOVERY_INTERVAL)
            .withDescription(
                "Default value is " +
                    DEFAULT_TOPIC_DISCOVERY_INTERVAL +
                    ". The interval (in ms) for the Pulsar source to discover the new topic partitions. A non-positive value disables the topic partition discovery. Note, This option only works if the 'topic-pattern' option is used.");

    public static final Option<String> TOPIC =
        Options.key("topic")
            .stringType()
            .noDefaultValue()
            .withDescription(
                "Topic name(s) to read data from when the table is used as source. It also supports topic list for source by separating topic by semicolon like 'topic-1;topic-2'. Note, only one of \"topic-pattern\" and \"topic\" can be specified for sources.");

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
                "Default value is " +
                    DEFAULT_POLL_TIMEOUT +
                    ". The maximum time (in ms) to wait when fetching records. A longer time increases throughput but also latency.");

    public static final Option<Long> POLL_INTERVAL =
        Options.key("poll.interval")
            .longType()
            .defaultValue(DEFAULT_POLL_INTERVAL)
            .withDescription(
                "Default value is " +
                    DEFAULT_POLL_INTERVAL +
                    ". The interval time(in ms) when fetcing records. A shorter time increases throughput, but also increases CPU load.");

    public static final Option<Integer> POLL_BATCH_SIZE =
        Options.key("poll.batch.size")
            .intType()
            .defaultValue(DEFAULT_POLL_BATCH_SIZE)
            .withDescription(
                "Default value is " +
                    DEFAULT_POLL_BATCH_SIZE +
                    ". The maximum number of records to fetch to wait when polling. A longer time increases throughput but also latency");

    public static final Option<SourceProperties.StartMode> CURSOR_STARTUP_MODE =
        Options.key("cursor.startup.mode")
            .enumType(SourceProperties.StartMode.class)
            .defaultValue(StartMode.LATEST)
            .withDescription(
                "Startup mode for Pulsar consumer, valid values are 'EARLIEST', 'LATEST', 'SUBSCRIPTION', 'TIMESTAMP'.");

    public static final Option<SourceProperties.StartMode> CURSOR_RESET_MODE =
        Options.key("cursor.reset.mode")
            .enumType(SourceProperties.StartMode.class)
            .noDefaultValue()
            .withDescription(
                "Cursor reset strategy for Pulsar consumer valid values are 'EARLIEST', 'LATEST'. Note, This option only works if the \"cursor.startup.mode\" option used 'SUBSCRIPTION'.");

    public static final Option<Long> CURSOR_STARTUP_TIMESTAMP =
        Options.key("cursor.startup.timestamp")
            .longType()
            .noDefaultValue()
            .withDescription(
                "Start from the specified epoch timestamp (in milliseconds). Note, This option is required when the \"cursor.startup.mode\" option used 'TIMESTAMP'.");

    // No use parameter
    public static final String CURSOR_STARTUP_ID = "cursor.startup.id";

    public static final Option<SourceProperties.StopMode> CURSOR_STOP_MODE =
        Options.key("cursor.stop.mode")
            .enumType(SourceProperties.StopMode.class)
            .defaultValue(StopMode.NEVER)
            .withDescription(
                "Stop mode for Pulsar consumer, valid values are 'NEVER', 'LATEST' and 'TIMESTAMP'. Note, When 'NEVER' is specified, it is a real-time job, and other mode are off-line jobs.");

    public static final Option<Long> CURSOR_STOP_TIMESTAMP =
        Options.key("cursor.stop.timestamp")
            .longType()
            .noDefaultValue()
            .withDescription("Stop from the specified epoch timestamp (in milliseconds)");

    /**
     * Startup mode for the pulsar consumer, see {@link #CURSOR_STARTUP_MODE}.
     */
    public enum StartMode {
        /**
         * Start from the earliest cursor possible.
         */
        EARLIEST,
        /**
         * Start from the latest cursor.
         */
        LATEST,
        /**
         * Start from committed cursors in a specific consumer subscription.
         */
        SUBSCRIPTION,
        /**
         * Start from user-supplied timestamp for each partition.
         */
        TIMESTAMP,
        /**
         * Start from user-supplied specific cursors for each partition.
         */
        SPECIFIC
    }

    /**
     * Stop mode for the pulsar consumer, see {@link #CURSOR_STOP_MODE}.
     */
    public enum StopMode {
        /**
         * Stop from the latest cursor.
         */
        LATEST,
        /**
         * Stop from user-supplied timestamp for each partition.
         */
        TIMESTAMP,
        /**
         * Stop from user-supplied specific cursors for each partition.
         */
        SPECIFIC,
        NEVER
    }
}

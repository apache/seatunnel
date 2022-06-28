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

public class SourceProperties {

    // Pulsar client API config prefix.
    public static final String CLIENT_CONFIG_PREFIX = "pulsar.client.";
    // Pulsar admin API config prefix.
    public static final String ADMIN_CONFIG_PREFIX = "pulsar.admin.";

    // --------------------------------------------------------------------------------------------
    // The configuration for ClientConfigurationData part.
    // All the configuration listed below should have the pulsar.client prefix.
    // --------------------------------------------------------------------------------------------

    public static final String PULSAR_SERVICE_URL = CLIENT_CONFIG_PREFIX + "serviceUrl";
    public static final String PULSAR_AUTH_PLUGIN_CLASS_NAME = CLIENT_CONFIG_PREFIX + "authPluginClassName";
    public static final String PULSAR_AUTH_PARAMS = CLIENT_CONFIG_PREFIX + "authParams";

    // --------------------------------------------------------------------------------------------
    // The configuration for ClientConfigurationData part.
    // All the configuration listed below should have the pulsar.client prefix.
    // --------------------------------------------------------------------------------------------

    public static final String PULSAR_ADMIN_URL = ADMIN_CONFIG_PREFIX + "adminUrl";

    // Pulsar source connector config prefix.
    public static final String SOURCE_CONFIG_PREFIX = "pulsar.source.";
    // Pulsar consumer API config prefix.
    public static final String CONSUMER_CONFIG_PREFIX = "pulsar.consumer.";

    // --------------------------------------------------------------------------------------------
    // The configuration for ConsumerConfigurationData part.
    // All the configuration listed below should have the pulsar.consumer prefix.
    // --------------------------------------------------------------------------------------------

    public static final String PULSAR_SUBSCRIPTION_NAME = CONSUMER_CONFIG_PREFIX + "subscriptionName";
    public static final String PULSAR_SUBSCRIPTION_TYPE = CONSUMER_CONFIG_PREFIX + "subscriptionType";
    public static final String PULSAR_SUBSCRIPTION_MODE = CONSUMER_CONFIG_PREFIX + "subscriptionMode";

    // --------------------------------------------------------------------------------------------
    // The configuration for pulsar source part.
    // All the configuration listed below should have the pulsar.source prefix.
    // --------------------------------------------------------------------------------------------

    public static final String PULSAR_PARTITION_DISCOVERY_INTERVAL_MS = SOURCE_CONFIG_PREFIX + "partitionDiscoveryIntervalMs";
    public static final String PULSAR_TOPIC = SOURCE_CONFIG_PREFIX + "topic";
    public static final String PULSAR_TOPIC_PATTERN = SOURCE_CONFIG_PREFIX + "topic.pattern";
    public static final String PULSAR_POLL_TIMEOUT = SOURCE_CONFIG_PREFIX + "poll.timeout";
    public static final String PULSAR_POLL_INTERVAL = SOURCE_CONFIG_PREFIX + "poll.interval";
    public static final String PULSAR_BATCH_SIZE = SOURCE_CONFIG_PREFIX + "batch.size";
    public static final String PULSAR_CURSOR_START_MODE = SOURCE_CONFIG_PREFIX + "scan.cursor.start.mode";
    public static final String PULSAR_CURSOR_START_RESET_MODE = SOURCE_CONFIG_PREFIX + "scan.cursor.start.reset.mode";
    public static final String PULSAR_CURSOR_START_TIMESTAMP = SOURCE_CONFIG_PREFIX + "scan.cursor.start.timestamp";
    public static final String PULSAR_CURSOR_START_ID = SOURCE_CONFIG_PREFIX + "scan.cursor.start.id";
    public static final String PULSAR_CURSOR_STOP_MODE = SOURCE_CONFIG_PREFIX + "scan.cursor.stop.mode";
    public static final String PULSAR_CURSOR_STOP_TIMESTAMP = SOURCE_CONFIG_PREFIX + "scan.cursor.stop.timestamp";

    /**
     * Startup mode for the Kafka consumer, see {@link #PULSAR_CURSOR_START_MODE}.
     */
    public enum StartMode {
        /**
         * Start from the earliest cursor possible.
         */
        EARLIEST,
        /**
         * "Start from the latest cursor."
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
        SPECIFIC;
    }

    /**
     * Startup mode for the Kafka consumer, see {@link #PULSAR_CURSOR_START_MODE}.
     */
    public enum StopMode {
        /**
         * "Start from the latest cursor."
         */
        LATEST,
        /**
         * Start from user-supplied timestamp for each partition.
         */
        TIMESTAMP,
        /**
         * Start from user-supplied specific cursors for each partition.
         */
        SPECIFIC,
        NEVER;
    }
}

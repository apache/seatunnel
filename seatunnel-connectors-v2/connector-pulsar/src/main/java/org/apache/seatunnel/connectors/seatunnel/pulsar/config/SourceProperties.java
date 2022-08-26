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

    // --------------------------------------------------------------------------------------------
    // The configuration for ClientConfigurationData part.
    // --------------------------------------------------------------------------------------------

    public static final String CLIENT_SERVICE_URL = "client.service-url";
    public static final String AUTH_PLUGIN_CLASS = "auth.plugin-class";
    public static final String AUTH_PARAMS = "auth.params";

    // --------------------------------------------------------------------------------------------
    // The configuration for ClientConfigurationData part.
    // All the configuration listed below should have the pulsar.client prefix.
    // --------------------------------------------------------------------------------------------

    public static final String ADMIN_SERVICE_URL = "admin.service-url";

    // --------------------------------------------------------------------------------------------
    // The configuration for ConsumerConfigurationData part.
    // --------------------------------------------------------------------------------------------

    public static final String SUBSCRIPTION_NAME = "subscription.name";
    public static final String SUBSCRIPTION_TYPE = "subscription.type";
    public static final String SUBSCRIPTION_MODE = "subscription.mode";

    // --------------------------------------------------------------------------------------------
    // The configuration for pulsar source part.
    // --------------------------------------------------------------------------------------------

    public static final String TOPIC_DISCOVERY_INTERVAL = "topic-discovery.interval";
    public static final String TOPIC = "topic";
    public static final String TOPIC_PATTERN = "topic-pattern";
    public static final String POLL_TIMEOUT = "poll.timeout";
    public static final String POLL_INTERVAL = "poll.interval";
    public static final String POLL_BATCH_SIZE = "poll.batch.size";
    public static final String CURSOR_STARTUP_MODE = "cursor.startup.mode";
    public static final String CURSOR_RESET_MODE = "cursor.reset.mode";
    public static final String CURSOR_STARTUP_TIMESTAMP = "cursor.startup.timestamp";
    public static final String CURSOR_STARTUP_ID = "cursor.startup.id";
    public static final String CURSOR_STOP_MODE = "cursor.stop.mode";
    public static final String CURSOR_STOP_TIMESTAMP = "cursor.stop.timestamp";

    /**
     * Startup mode for the pulsar consumer, see {@link #CURSOR_STARTUP_MODE}.
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
     * Startup mode for the pulsar consumer, see {@link #CURSOR_STARTUP_MODE}.
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

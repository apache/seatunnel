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

package org.apache.seatunnel.connectors.seatunnel.azure.queue.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

public class AzureQueueStorageSourceOptions extends ConnectorCommonOptions {

    public static final Option<String> QUEUE_NAME = AzureQueueStorageSinkOptions.QUEUE_NAME;
    public static final Option<AuthenticationType> AUTHENTICATION_TYPE =
            AzureQueueStorageSinkOptions.AUTHENTICATION_TYPE;
    public static final Option<String> CONNECTION_STRING =
            AzureQueueStorageSinkOptions.CONNECTION_STRING;
    public static final Option<String> ENDPOINT = AzureQueueStorageSinkOptions.ENDPOINT;
    public static final Option<String> ACCOUNT_NAME = AzureQueueStorageSinkOptions.ACCOUNT_NAME;
    public static final Option<String> ACCOUNT_KEY = AzureQueueStorageSinkOptions.ACCOUNT_KEY;
    public static final Option<String> SAS_TOKEN = AzureQueueStorageSinkOptions.SAS_TOKEN;
    public static final Option<MessageFormat> FORMAT = AzureQueueStorageSinkOptions.FORMAT;
    public static final Option<String> FIELD_DELIMITER =
            AzureQueueStorageSinkOptions.FIELD_DELIMITER;
    public static final Option<MessageEncoding> MESSAGE_ENCODING =
            AzureQueueStorageSinkOptions.MESSAGE_ENCODING;

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(32)
                    .withDescription("Maximum number of messages requested in each receive call.");

    public static final Option<Integer> VISIBILITY_TIMEOUT_SECONDS =
            Options.key("visibility_timeout_seconds")
                    .intType()
                    .defaultValue(300)
                    .withDescription(
                            "Initial message invisibility period in seconds. The connector renews it until checkpoint acknowledgement.");

    public static final Option<Long> POLL_INTERVAL_MS =
            Options.key("poll_interval_ms")
                    .longType()
                    .defaultValue(1_000L)
                    .withDescription("Delay in milliseconds before polling an empty queue again.");

    public static final Option<Integer> MAX_IN_FLIGHT_MESSAGES =
            Options.key("max_in_flight_messages")
                    .intType()
                    .defaultValue(1_000)
                    .withDescription(
                            "Maximum number of received messages retained until a checkpoint completes.");

    public static final Option<Long> OPERATION_TIMEOUT_MS =
            Options.key("operation_timeout_ms")
                    .longType()
                    .defaultValue(60_000L)
                    .withDescription(
                            "Maximum time in milliseconds for an Azure Queue receive, visibility update or delete request.");

    private AzureQueueStorageSourceOptions() {}
}

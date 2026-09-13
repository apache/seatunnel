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
package org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

public class AzureEventHubsSourceOptions extends ConnectorCommonOptions {

    public static final String CONNECTOR_IDENTITY = "AzureEventHubs";

    public static final Option<String> CONNECTION_STRING =
            Options.key("connection_string")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Azure Event Hubs namespace connection string. The value must not include EntityPath.");

    public static final Option<String> EVENT_HUB_NAME =
            Options.key("event_hub_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the Event Hub to consume.");

    public static final Option<String> CONSUMER_GROUP =
            Options.key("consumer_group")
                    .stringType()
                    .defaultValue("$Default")
                    .withDescription("Consumer group used by the source.");

    public static final Option<AzureEventHubsStartMode> START_MODE =
            Options.key("start_mode")
                    .enumType(AzureEventHubsStartMode.class)
                    .defaultValue(AzureEventHubsStartMode.EARLIEST)
                    .withDescription(
                            "Position used only for a fresh job. Supported values are earliest and latest. Restored jobs always use the checkpointed sequence number.");

    public static final Option<AzureEventHubsMessageFormat> FORMAT =
            Options.key("format")
                    .enumType(AzureEventHubsMessageFormat.class)
                    .defaultValue(AzureEventHubsMessageFormat.JSON)
                    .withDescription("Event body format. Supported values are json and text.");

    public static final Option<String> FIELD_DELIMITER =
            Options.key("field_delimiter")
                    .stringType()
                    .defaultValue(",")
                    .withDescription("Field delimiter used when format is text.");

    public static final Option<Integer> MAX_BATCH_SIZE =
            Options.key("max_batch_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Maximum number of events returned by one partition poll.");

    public static final Option<Long> POLL_TIMEOUT_MS =
            Options.key("poll_timeout_ms")
                    .longType()
                    .defaultValue(1_000L)
                    .withDescription(
                            "Maximum time in milliseconds that one partition poll waits for events.");

    public static final Option<Integer> PREFETCH_COUNT =
            Options.key("prefetch_count")
                    .intType()
                    .defaultValue(300)
                    .withDescription(
                            "Maximum number of events the Azure SDK prefetches for each partition assigned to a source reader.");

    private AzureEventHubsSourceOptions() {}
}

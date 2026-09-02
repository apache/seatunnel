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
package org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum AzureEventHubsConnectorErrorCode implements SeaTunnelErrorCode {
    CONFIGURATION_FAILED("AzureEventHubs-01", "Azure Event Hubs configuration is invalid"),
    CONNECTION_FAILED("AzureEventHubs-02", "Create Azure Event Hubs client failed"),
    PARTITION_DISCOVERY_FAILED("AzureEventHubs-03", "Discover Event Hubs partitions failed"),
    READ_FAILED("AzureEventHubs-04", "Read Azure Event Hubs event failed"),
    DESERIALIZATION_FAILED("AzureEventHubs-05", "Deserialize Azure Event Hubs event failed"),
    CLOSE_FAILED("AzureEventHubs-06", "Close Azure Event Hubs client failed");

    private final String code;
    private final String description;

    AzureEventHubsConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}

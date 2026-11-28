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

package org.apache.seatunnel.connectors.seatunnel.azureiothub;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.APPLICATION_PROPERTIES;
import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.DEVICE_ID;
import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.FIELD;
import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.HUB_URL;
import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.MAX_PAYLOAD;
import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.READ_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.ROW_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.SAS_TTL;
import static org.apache.seatunnel.connectors.seatunnel.azureiothub.AzureIoTHubSinkOptions.SECRET_KEY;

/**
 * Registers only the dedicated device-to-cloud sink. Configuration is validated before a worker
 * creates network resources; the Factory owns neither sockets nor pending delivery state.
 */
@AutoService(Factory.class)
public final class AzureIoTHubSinkFactory
        implements TableSinkFactory<SeaTunnelRow, Void, Void, Void> {
    static final String IDENTIFIER = "AzureIoTHub";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HUB_URL, DEVICE_ID, SECRET_KEY)
                .optional(
                        FORMAT,
                        FIELD,
                        APPLICATION_PROPERTIES,
                        SAS_TTL,
                        MAX_PAYLOAD,
                        CONNECT_TIMEOUT,
                        READ_TIMEOUT,
                        ROW_TIMEOUT,
                        MAX_ATTEMPTS)
                .build();
    }

    @Override
    public TableSink<SeaTunnelRow, Void, Void, Void> createSink(TableSinkFactoryContext context) {
        return () -> new AzureIoTHubSink(context.getOptions(), context.getCatalogTable());
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.azure.queue.sink;

import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.AuthenticationType;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.AzureQueueSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.AzureQueueStorageSinkOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class AzureQueueStorageSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return AzureQueueStorageSink.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        AzureQueueStorageSinkOptions.QUEUE_NAME,
                        AzureQueueStorageSinkOptions.AUTHENTICATION_TYPE)
                .conditional(
                        AzureQueueStorageSinkOptions.AUTHENTICATION_TYPE,
                        AuthenticationType.CONNECTION_STRING,
                        AzureQueueStorageSinkOptions.CONNECTION_STRING)
                .conditional(
                        AzureQueueStorageSinkOptions.AUTHENTICATION_TYPE,
                        AuthenticationType.SHARED_KEY,
                        AzureQueueStorageSinkOptions.ENDPOINT,
                        AzureQueueStorageSinkOptions.ACCOUNT_NAME,
                        AzureQueueStorageSinkOptions.ACCOUNT_KEY)
                .conditional(
                        AzureQueueStorageSinkOptions.AUTHENTICATION_TYPE,
                        AuthenticationType.SAS_TOKEN,
                        AzureQueueStorageSinkOptions.ENDPOINT,
                        AzureQueueStorageSinkOptions.SAS_TOKEN)
                .optional(
                        AzureQueueStorageSinkOptions.FORMAT,
                        AzureQueueStorageSinkOptions.FIELD_DELIMITER,
                        AzureQueueStorageSinkOptions.MESSAGE_ENCODING)
                .optional(
                        AzureQueueStorageSinkOptions.MAX_IN_FLIGHT,
                        Conditions.greaterThan(AzureQueueStorageSinkOptions.MAX_IN_FLIGHT, 0))
                .optional(
                        AzureQueueStorageSinkOptions.OPERATION_TIMEOUT_MS,
                        Conditions.greaterThan(
                                AzureQueueStorageSinkOptions.OPERATION_TIMEOUT_MS, 0L))
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () ->
                new AzureQueueStorageSink(
                        AzureQueueSinkConfig.from(context.getOptions()), context.getCatalogTable());
    }
}

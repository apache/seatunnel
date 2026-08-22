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
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.AzureQueueSinkOptions;

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
                        AzureQueueSinkOptions.QUEUE_NAME, AzureQueueSinkOptions.AUTHENTICATION_TYPE)
                .conditional(
                        AzureQueueSinkOptions.AUTHENTICATION_TYPE,
                        AuthenticationType.CONNECTION_STRING,
                        AzureQueueSinkOptions.CONNECTION_STRING)
                .conditional(
                        AzureQueueSinkOptions.AUTHENTICATION_TYPE,
                        AuthenticationType.SHARED_KEY,
                        AzureQueueSinkOptions.ENDPOINT,
                        AzureQueueSinkOptions.ACCOUNT_NAME,
                        AzureQueueSinkOptions.ACCOUNT_KEY)
                .conditional(
                        AzureQueueSinkOptions.AUTHENTICATION_TYPE,
                        AuthenticationType.SAS_TOKEN,
                        AzureQueueSinkOptions.ENDPOINT,
                        AzureQueueSinkOptions.SAS_TOKEN)
                .optional(
                        AzureQueueSinkOptions.FORMAT,
                        AzureQueueSinkOptions.FIELD_DELIMITER,
                        AzureQueueSinkOptions.MESSAGE_ENCODING)
                .optional(
                        AzureQueueSinkOptions.MAX_IN_FLIGHT,
                        Conditions.greaterThan(AzureQueueSinkOptions.MAX_IN_FLIGHT, 0))
                .optional(
                        AzureQueueSinkOptions.OPERATION_TIMEOUT_MS,
                        Conditions.greaterThan(AzureQueueSinkOptions.OPERATION_TIMEOUT_MS, 0L))
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () ->
                new AzureQueueStorageSink(
                        AzureQueueSinkConfig.from(context.getOptions()), context.getCatalogTable());
    }
}

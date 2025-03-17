/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *     contributor license agreements.  See the NOTICE file distributed with
 *     this work for additional information regarding copyright ownership.
 *     The ASF licenses this file to You under the Apache License, Version 2.0
 *     (the "License"); you may not use this file except in compliance with
 *     the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqSinkOptions;

import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.Map;

@AutoService(Factory.class)
public class RabbitmqSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "RabbitMQ";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        RabbitmqSinkOptions.HOST,
                        RabbitmqSinkOptions.PORT,
                        RabbitmqSinkOptions.VIRTUAL_HOST,
                        RabbitmqSinkOptions.QUEUE_NAME)
                .bundled(RabbitmqSinkOptions.USERNAME, RabbitmqSinkOptions.PASSWORD)
                .optional(
                        RabbitmqSinkOptions.URL,
                        RabbitmqSinkOptions.ROUTING_KEY,
                        RabbitmqSinkOptions.EXCHANGE,
                        RabbitmqSinkOptions.NETWORK_RECOVERY_INTERVAL,
                        RabbitmqSinkOptions.TOPOLOGY_RECOVERY_ENABLED,
                        RabbitmqSinkOptions.AUTOMATIC_RECOVERY_ENABLED,
                        RabbitmqSinkOptions.CONNECTION_TIMEOUT,
                        RabbitmqSinkOptions.FOR_E2E_TESTING,
                        RabbitmqSinkOptions.DURABLE,
                        RabbitmqSinkOptions.EXCLUSIVE,
                        RabbitmqSinkOptions.AUTO_DELETE,
                        RabbitmqSinkOptions.RABBITMQ_CONFIG)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        CatalogTable catalogTable = context.getCatalogTable();
        ReadonlyConfig finalConfig =
                generateCurrentReadonlyConfig(context.getOptions(), catalogTable);
        return () -> new RabbitmqSink(new RabbitmqConfig(finalConfig), catalogTable);
    }

    private ReadonlyConfig generateCurrentReadonlyConfig(
            ReadonlyConfig config, CatalogTable catalogTable) {
        // Copy the configuration map to make modifications.
        Map<String, Object> configMap = new HashMap<>(config.toMap());

        // Replace placeholders in the queue name option as an example.
        config.getOptional(RabbitmqSinkOptions.QUEUE_NAME)
                .ifPresent(
                        originalQueueName -> {
                            String replacedQueueName =
                                    replaceCatalogTableInPath(originalQueueName, catalogTable);
                            configMap.put(RabbitmqSinkOptions.QUEUE_NAME.key(), replacedQueueName);
                        });

        // Similarly, you can process other options (e.g., ROUTING_KEY) if required.
        config.getOptional(RabbitmqSinkOptions.ROUTING_KEY)
                .ifPresent(
                        originalRoutingKey -> {
                            String replacedRoutingKey =
                                    replaceCatalogTableInPath(originalRoutingKey, catalogTable);
                            configMap.put(
                                    RabbitmqSinkOptions.ROUTING_KEY.key(), replacedRoutingKey);
                        });

        return ReadonlyConfig.fromMap(configMap);
    }

    private String replaceCatalogTableInPath(String origin, CatalogTable catalogTable) {
        String replaced = origin;
        TableIdentifier tableIdentifier = catalogTable.getTableId();
        if (tableIdentifier != null) {
            if (tableIdentifier.getSchemaName() != null) {
                replaced = replaced.replace("${schema}", tableIdentifier.getSchemaName());
            }
            if (tableIdentifier.getTableName() != null) {
                replaced = replaced.replace("${table}", tableIdentifier.getTableName());
            }
        }
        return replaced;
    }
}

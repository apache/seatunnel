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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Pure schema construction shared by the source runtime and connectivity preflight. */
final class RabbitmqSourceSchema {
    private RabbitmqSourceSchema() {}

    /**
     * Parses the configuration to initialize the CatalogTables. Determines whether the source is
     * operating in Single-Table or Multi-Table mode.
     *
     * @param config The plugin configuration.
     */
    static void initializeCatalogTables(
            ReadonlyConfig config,
            RabbitmqConfig rabbitmqConfig,
            List<CatalogTable> catalogTables,
            Map<String, CatalogTable> queueToTableMap) {
        boolean hasTableConfigs =
                config.getOptional(ConnectorCommonOptions.TABLE_CONFIGS).isPresent();
        boolean hasSchema = config.getOptional(ConnectorCommonOptions.SCHEMA).isPresent();

        if (hasTableConfigs) {
            // Multi-Table Mode: Parse multiple queue configurations
            List<Map<String, Object>> tableConfigList =
                    config.get(ConnectorCommonOptions.TABLE_CONFIGS);
            for (Map<String, Object> item : tableConfigList) {
                ReadonlyConfig tableConfig = ReadonlyConfig.fromMap(item);
                CatalogTable table = buildCatalogTable(tableConfig);
                String queueName = tableConfig.get(RabbitmqBaseOptions.QUEUE_NAME);

                catalogTables.add(table);
                queueToTableMap.put(queueName, table);
            }
        } else if (hasSchema) {
            CatalogTable table = buildCatalogTable(config);
            String queueName = config.get(RabbitmqBaseOptions.QUEUE_NAME);
            if (queueName == null) {
                queueName = rabbitmqConfig.getQueueName();
            }
            catalogTables.add(table);
            queueToTableMap.put(queueName, table);
        }
    }

    private static CatalogTable buildCatalogTable(ReadonlyConfig config) {
        CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(config);
        Map<String, String> options = new HashMap<>(catalogTable.getOptions());
        options.put(
                RabbitmqBaseOptions.FORMAT.key(), config.get(RabbitmqBaseOptions.FORMAT).name());
        config.getOptional(RabbitmqBaseOptions.PROTOBUF_SCHEMA)
                .ifPresent(value -> options.put(RabbitmqBaseOptions.PROTOBUF_SCHEMA.key(), value));
        config.getOptional(RabbitmqBaseOptions.PROTOBUF_MESSAGE_NAME)
                .ifPresent(
                        value ->
                                options.put(
                                        RabbitmqBaseOptions.PROTOBUF_MESSAGE_NAME.key(), value));
        return CatalogTable.of(
                TableIdentifier.of(catalogTable.getCatalogName(), catalogTable.getTablePath()),
                catalogTable.getTableSchema(),
                options,
                catalogTable.getPartitionKeys(),
                catalogTable.getComment(),
                catalogTable.getCatalogName(),
                catalogTable.getMetadataSchema());
    }
}

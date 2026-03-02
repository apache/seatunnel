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

package org.apache.seatunnel.connectors.seatunnel.redis.source;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisTableConfig;
import org.apache.seatunnel.connectors.seatunnel.redis.exception.RedisConnectorException;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.text.TextDeserializationSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    private final RedisParameters redisParameters = new RedisParameters();
    private Map<TablePath, RedisSourceTable> sourceTablesMap;

    @Override
    public String getPluginName() {
        return RedisBaseOptions.CONNECTOR_IDENTITY;
    }

    public RedisSource(ReadonlyConfig readonlyConfig) {
        this.redisParameters.buildWithConfig(readonlyConfig);
        this.sourceTablesMap = createSourceTablesMap(readonlyConfig);
    }

    /**
     * Create source tables map from configuration, supporting both single and multi-table modes.
     *
     * @param readonlyConfig Configuration
     * @return Map of TablePath to RedisSourceTable
     */
    private Map<TablePath, RedisSourceTable> createSourceTablesMap(ReadonlyConfig readonlyConfig) {
        List<RedisTableConfig> tableConfigs = RedisTableConfig.of(readonlyConfig);
        Map<TablePath, RedisSourceTable> tablesMap = new HashMap<>();

        for (RedisTableConfig tableConfig : tableConfigs) {
            TablePath tablePath = tableConfig.getTablePath(readonlyConfig, tableConfig.getKeys());

            // Check for duplicate TablePath
            if (tablesMap.containsKey(tablePath)) {
                throw new RedisConnectorException(
                        CommonErrorCode.ILLEGAL_ARGUMENT,
                        String.format(
                                "Duplicate table_path found: %s. Please ensure each table configuration has a unique table_path.",
                                tablePath));
            }

            RedisSourceTable sourceTable =
                    createSourceTable(readonlyConfig, tableConfig, tablePath);
            tablesMap.put(tablePath, sourceTable);
        }

        return tablesMap;
    }

    /**
     * Create a single source table from table configuration.
     *
     * @param readonlyConfig readonly config
     * @param tableConfig Table-specific configuration
     * @param tablePath TablePath for this table
     * @return RedisSourceTable
     */
    private RedisSourceTable createSourceTable(
            ReadonlyConfig readonlyConfig, RedisTableConfig tableConfig, TablePath tablePath) {
        CatalogTable catalogTable;
        DeserializationSchema<SeaTunnelRow> deserializationSchema;

        // Create catalog table and deserialization schema based on format
        if (tableConfig.getSchema() != null) {
            // Build catalog table from config
            catalogTable = CatalogTableUtil.buildWithConfig(readonlyConfig);
            SeaTunnelRowType seaTunnelRowType = catalogTable.getSeaTunnelRowType();

            // Create deserialization schema based on format
            switch (tableConfig.getFormat()) {
                case JSON:
                    deserializationSchema =
                            new JsonDeserializationSchema(catalogTable, false, false);
                    break;
                case TEXT:
                    deserializationSchema =
                            TextDeserializationSchema.builder()
                                    .seaTunnelRowType(seaTunnelRowType)
                                    .delimiter(tableConfig.getFieldDelimiter())
                                    .build();
                    break;
                default:
                    throw new RedisConnectorException(
                            SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                            String.format(
                                    "PluginName: %s, PluginType: %s, Message: %s",
                                    getPluginName(),
                                    PluginType.SOURCE,
                                    "Unsupported format: " + tableConfig.getFormat()));
            }
        } else {
            // No schema specified, use simple text table
            catalogTable = CatalogTableUtil.buildSimpleTextTable();
            deserializationSchema = null;
        }

        // Use toSourceTable method to convert configuration to source table
        // This encapsulates the conversion logic in RedisTableConfig,
        // reducing code duplication and improving maintainability
        return tableConfig.toSourceTable(tablePath, catalogTable, deserializationSchema);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        // Return all catalog tables from source tables map
        List<CatalogTable> catalogTables = new ArrayList<>(sourceTablesMap.size());
        for (RedisSourceTable sourceTable : sourceTablesMap.values()) {
            catalogTables.add(sourceTable.getCatalogTable());
        }
        return catalogTables;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new RedisSourceReader(redisParameters, readerContext, sourceTablesMap);
    }
}

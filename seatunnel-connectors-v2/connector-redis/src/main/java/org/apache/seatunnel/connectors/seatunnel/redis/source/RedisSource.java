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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisTableConfig;
import org.apache.seatunnel.connectors.seatunnel.redis.exception.RedisConnectorException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RedisSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    private final RedisParameters redisParameters = new RedisParameters();
    private Map<TablePath, RedisTableConfig> sourceTablesMap;

    @Override
    public String getPluginName() {
        return RedisBaseOptions.CONNECTOR_IDENTITY;
    }

    public RedisSource(ReadonlyConfig readonlyConfig) {
        this.redisParameters.buildConnectionConfig(readonlyConfig);
        this.sourceTablesMap = createSourceTablesMap(readonlyConfig);
    }

    /**
     * Create source tables map from configuration, supporting both single and multi-table modes.
     *
     * @param readonlyConfig Configuration
     * @return Map of TablePath to RedisTableConfig
     */
    private Map<TablePath, RedisTableConfig> createSourceTablesMap(ReadonlyConfig readonlyConfig) {
        List<RedisTableConfig> tableConfigs = RedisTableConfig.of(readonlyConfig);
        Map<TablePath, RedisTableConfig> tablesMap = new LinkedHashMap<>();

        for (RedisTableConfig tableConfig : tableConfigs) {
            TablePath tablePath = tableConfig.getTablePath();

            // Check for duplicate TablePath
            if (tablesMap.containsKey(tablePath)) {
                throw new RedisConnectorException(
                        CommonErrorCode.ILLEGAL_ARGUMENT,
                        String.format(
                                "Duplicate table_path found: %s. Please ensure each table configuration has a unique table_path.",
                                tablePath));
            }

            tablesMap.put(tablePath, tableConfig);
        }

        return tablesMap;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        List<CatalogTable> catalogTables = new ArrayList<>(sourceTablesMap.size());
        for (RedisTableConfig tableConfig : sourceTablesMap.values()) {
            catalogTables.add(tableConfig.getCatalogTable());
        }
        return catalogTables;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new RedisSourceReader(redisParameters, readerContext, sourceTablesMap);
    }
}

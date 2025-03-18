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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.catalog.MaxComputeCatalog;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.PARTITION_SPEC;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.PLUGIN_NAME;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.PROJECT;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.READ_COLUMNS;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.SPLIT_ROW;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.TABLE_LIST;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.TABLE_NAME;

@Slf4j
public class MaxcomputeSource
        implements SeaTunnelSource<SeaTunnelRow, MaxcomputeSourceSplit, MaxcomputeSourceState>,
                SupportParallelism,
                SupportColumnProjection {
    private final Map<TablePath, SourceTableInfo> sourceTableInfos;
    private ReadonlyConfig readonlyConfig;

    public MaxcomputeSource(ReadonlyConfig readonlyConfig) {
        this.readonlyConfig = readonlyConfig;
        this.sourceTableInfos = getSourceTableInfos(readonlyConfig);
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    private Map<TablePath, SourceTableInfo> getSourceTableInfos(ReadonlyConfig readonlyConfig) {
        Map<TablePath, SourceTableInfo> tables = new HashMap<>();

        if (readonlyConfig.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
            CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(readonlyConfig);
            catalogTable =
                    CatalogTable.of(
                            TableIdentifier.of(
                                    "maxcompute",
                                    readonlyConfig.get(PROJECT),
                                    readonlyConfig.get(TABLE_NAME)),
                            catalogTable);
            tables.put(
                    catalogTable.getTablePath(),
                    new SourceTableInfo(
                            catalogTable,
                            readonlyConfig.get(PARTITION_SPEC),
                            readonlyConfig.get(SPLIT_ROW)));
        } else {
            try (MaxComputeCatalog catalog = new MaxComputeCatalog("maxcompute", readonlyConfig)) {
                catalog.open();
                if (readonlyConfig.getOptional(TABLE_LIST).isPresent()) {
                    for (Map<String, Object> subConfig : readonlyConfig.get(TABLE_LIST)) {
                        ReadonlyConfig subReadonlyConfig = ReadonlyConfig.fromMap(subConfig);
                        String project =
                                subReadonlyConfig
                                        .getOptional(PROJECT)
                                        .orElse(readonlyConfig.get(PROJECT));
                        TablePath tablePath =
                                TablePath.of(project, subReadonlyConfig.get(TABLE_NAME));
                        String partitionSpec =
                                subReadonlyConfig
                                        .getOptional(PARTITION_SPEC)
                                        .orElse(readonlyConfig.get(PARTITION_SPEC));

                        if (subReadonlyConfig
                                .getOptional(ConnectorCommonOptions.SCHEMA)
                                .isPresent()) {
                            CatalogTable catalogTable =
                                    CatalogTableUtil.buildWithConfig(subReadonlyConfig);
                            catalogTable =
                                    CatalogTable.of(
                                            TableIdentifier.of("maxcompute", tablePath),
                                            catalogTable);
                            tables.put(
                                    catalogTable.getTablePath(),
                                    new SourceTableInfo(
                                            catalogTable,
                                            partitionSpec,
                                            subReadonlyConfig.get(SPLIT_ROW)));
                        } else {
                            Integer splitRow =
                                    subReadonlyConfig
                                            .getOptional(SPLIT_ROW)
                                            .orElse(readonlyConfig.get(SPLIT_ROW));
                            tables.put(
                                    tablePath,
                                    new SourceTableInfo(
                                            catalog.getTable(
                                                    tablePath, subReadonlyConfig.get(READ_COLUMNS)),
                                            partitionSpec,
                                            splitRow));
                        }
                    }
                } else {
                    TablePath tablePath =
                            TablePath.of(
                                    readonlyConfig.get(PROJECT), readonlyConfig.get(TABLE_NAME));
                    tables.put(
                            tablePath,
                            new SourceTableInfo(
                                    catalog.getTable(tablePath, readonlyConfig.get(READ_COLUMNS)),
                                    readonlyConfig.get(PARTITION_SPEC),
                                    readonlyConfig.get(SPLIT_ROW)));
                }
            }
        }
        return tables;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return sourceTableInfos.values().stream()
                .map(SourceTableInfo::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public SourceReader<SeaTunnelRow, MaxcomputeSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new MaxcomputeSourceReader(
                this.readonlyConfig, readerContext, this.sourceTableInfos);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceSplitEnumerator<MaxcomputeSourceSplit, MaxcomputeSourceState> createEnumerator(
            SourceSplitEnumerator.Context<MaxcomputeSourceSplit> enumeratorContext)
            throws Exception {
        return new MaxcomputeSourceSplitEnumerator(
                enumeratorContext, this.readonlyConfig, this.sourceTableInfos);
    }

    @Override
    public SourceSplitEnumerator<MaxcomputeSourceSplit, MaxcomputeSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<MaxcomputeSourceSplit> enumeratorContext,
            MaxcomputeSourceState checkpointState)
            throws Exception {
        return new MaxcomputeSourceSplitEnumerator(
                enumeratorContext, this.readonlyConfig, this.sourceTableInfos, checkpointState);
    }
}

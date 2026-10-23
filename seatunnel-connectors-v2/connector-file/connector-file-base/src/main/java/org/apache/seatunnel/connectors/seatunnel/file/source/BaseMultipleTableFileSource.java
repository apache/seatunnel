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

package org.apache.seatunnel.connectors.seatunnel.file.source;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseMultipleTableFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileDiscoveryMode;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FilePostSyncAction;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.MultipleTableFileSourceReader;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.ContinuousMultipleTableFileSourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.DefaultFileSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSplitStrategyFactory;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.MultipleTableFileSourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.MultipleTableFileSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.state.FileSourceState;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class BaseMultipleTableFileSource
        implements SeaTunnelSource<SeaTunnelRow, FileSourceSplit, FileSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private final BaseMultipleTableFileSourceConfig baseMultipleTableFileSourceConfig;
    private final FileSplitStrategy fileSplitStrategy;

    public BaseMultipleTableFileSource(
            BaseMultipleTableFileSourceConfig baseMultipleTableFileSourceConfig) {
        this.baseMultipleTableFileSourceConfig = baseMultipleTableFileSourceConfig;
        this.fileSplitStrategy = new DefaultFileSplitStrategy();
    }

    public BaseMultipleTableFileSource(
            BaseMultipleTableFileSourceConfig baseMultipleTableFileSourceConfig,
            FileSplitStrategy fileSplitStrategy) {
        this.baseMultipleTableFileSourceConfig = baseMultipleTableFileSourceConfig;
        this.fileSplitStrategy = fileSplitStrategy;
    }

    protected static FileSplitStrategy initFileSplitStrategy(
            BaseMultipleTableFileSourceConfig sourceConfig) {
        Map<String, FileSplitStrategy> splitStrategies = new HashMap<>();
        for (BaseFileSourceConfig fileSourceConfig : sourceConfig.getFileSourceConfigs()) {
            String tableId =
                    fileSourceConfig.getCatalogTable().getTableId().toTablePath().toString();
            splitStrategies.put(
                    tableId,
                    FileSplitStrategyFactory.initFileSplitStrategy(
                            fileSourceConfig.getBaseFileSourceConfig(),
                            fileSourceConfig.getHadoopConfig()));
        }
        return new MultipleTableFileSplitStrategy(splitStrategies);
    }

    @Override
    public Boundedness getBoundedness() {
        return resolveDiscoveryMode() == FileDiscoveryMode.CONTINUOUS
                ? Boundedness.UNBOUNDED
                : Boundedness.BOUNDED;
    }

    @Override
    public abstract String getPluginName();

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return baseMultipleTableFileSourceConfig.getFileSourceConfigs().stream()
                .map(BaseFileSourceConfig::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public SourceReader<SeaTunnelRow, FileSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new MultipleTableFileSourceReader(readerContext, baseMultipleTableFileSourceConfig);
    }

    @Override
    public SourceSplitEnumerator<FileSourceSplit, FileSourceState> createEnumerator(
            SourceSplitEnumerator.Context<FileSourceSplit> enumeratorContext) {
        if (resolveDiscoveryMode() == FileDiscoveryMode.CONTINUOUS) {
            return new ContinuousMultipleTableFileSourceSplitEnumerator(
                    enumeratorContext, baseMultipleTableFileSourceConfig, fileSplitStrategy);
        }
        return new MultipleTableFileSourceSplitEnumerator(
                enumeratorContext,
                baseMultipleTableFileSourceConfig,
                fileSplitStrategy,
                resolveDocumentRoutingTableIds());
    }

    @Override
    public SourceSplitEnumerator<FileSourceSplit, FileSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<FileSourceSplit> enumeratorContext,
            FileSourceState checkpointState) {
        if (resolveDiscoveryMode() == FileDiscoveryMode.CONTINUOUS) {
            return new ContinuousMultipleTableFileSourceSplitEnumerator(
                    enumeratorContext,
                    baseMultipleTableFileSourceConfig,
                    fileSplitStrategy,
                    checkpointState);
        }
        return new MultipleTableFileSourceSplitEnumerator(
                enumeratorContext,
                baseMultipleTableFileSourceConfig,
                fileSplitStrategy,
                resolveDocumentRoutingTableIds(),
                checkpointState);
    }

    private Set<String> resolveDocumentRoutingTableIds() {
        Set<String> tableIds = new HashSet<>();
        for (BaseFileSourceConfig config :
                baseMultipleTableFileSourceConfig.getFileSourceConfigs()) {
            if (config.getFileFormat() == FileFormat.MARKDOWN
                    && config.getBaseFileSourceConfig()
                            .get(FileBaseSourceOptions.MARKDOWN_RAG_METADATA_ENABLED)) {
                tableIds.add(config.getCatalogTable().getTableId().toTablePath().toString());
            }
        }
        return tableIds;
    }

    private FileDiscoveryMode resolveDiscoveryMode() {
        List<BaseFileSourceConfig> configs =
                baseMultipleTableFileSourceConfig.getFileSourceConfigs();
        if (configs == null || configs.isEmpty()) {
            return FileDiscoveryMode.ONCE;
        }
        FileDiscoveryMode mode =
                configs.get(0).getBaseFileSourceConfig().get(FileBaseSourceOptions.DISCOVERY_MODE);
        for (BaseFileSourceConfig config : configs) {
            FileDiscoveryMode currentMode =
                    config.getBaseFileSourceConfig().get(FileBaseSourceOptions.DISCOVERY_MODE);
            if (currentMode != mode) {
                throw new FileConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "In multi-table mode, option '"
                                + FileBaseSourceOptions.DISCOVERY_MODE.key()
                                + "' must be consistent across tables.");
            }
        }
        if (mode != FileDiscoveryMode.CONTINUOUS) {
            for (BaseFileSourceConfig config : configs) {
                FilePostSyncAction action =
                        config.getBaseFileSourceConfig()
                                .get(FileBaseSourceOptions.POST_SYNC_ACTION);
                if (action == FilePostSyncAction.NONE) {
                    continue;
                }
                throw new FileConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "post_sync_action only supports discovery_mode=continuous. "
                                + "Please set post_sync_action=none or switch discovery_mode to continuous.");
            }
        }
        return mode;
    }
}

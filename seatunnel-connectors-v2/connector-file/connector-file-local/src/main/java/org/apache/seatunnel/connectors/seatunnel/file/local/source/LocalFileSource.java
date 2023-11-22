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

package org.apache.seatunnel.connectors.seatunnel.file.local.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.local.source.config.LocalFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.local.source.config.MultipleTableLocalFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.local.source.reader.MultipleTableLocalFileSourceReader;
import org.apache.seatunnel.connectors.seatunnel.file.local.source.split.LocalFileSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.file.local.source.split.MultipleTableLocalFileSourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.file.local.source.state.LocalFileSourceState;

import java.util.List;
import java.util.stream.Collectors;

public class LocalFileSource
        implements SeaTunnelSource<SeaTunnelRow, LocalFileSourceSplit, LocalFileSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private final MultipleTableLocalFileSourceConfig multipleTableLocalFileSourceConfig;

    public LocalFileSource(ReadonlyConfig readonlyConfig) {
        this.multipleTableLocalFileSourceConfig =
                new MultipleTableLocalFileSourceConfig(readonlyConfig);
    }

    @Override
    public String getPluginName() {
        return FileSystemType.LOCAL.getFileSystemPluginName();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return multipleTableLocalFileSourceConfig.getLocalFileSourceConfigs().stream()
                .map(LocalFileSourceConfig::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public SourceReader<SeaTunnelRow, LocalFileSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new MultipleTableLocalFileSourceReader(
                readerContext, multipleTableLocalFileSourceConfig);
    }

    @Override
    public SourceSplitEnumerator<LocalFileSourceSplit, LocalFileSourceState> createEnumerator(
            SourceSplitEnumerator.Context<LocalFileSourceSplit> enumeratorContext) {
        return new MultipleTableLocalFileSourceSplitEnumerator(
                enumeratorContext, multipleTableLocalFileSourceConfig);
    }

    @Override
    public SourceSplitEnumerator<LocalFileSourceSplit, LocalFileSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<LocalFileSourceSplit> enumeratorContext,
            LocalFileSourceState checkpointState) {
        return new MultipleTableLocalFileSourceSplitEnumerator(
                enumeratorContext, multipleTableLocalFileSourceConfig, checkpointState);
    }
}

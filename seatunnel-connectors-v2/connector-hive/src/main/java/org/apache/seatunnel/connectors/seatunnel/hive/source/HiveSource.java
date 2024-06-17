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

package org.apache.seatunnel.connectors.seatunnel.hive.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.BaseHdfsFileSource;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.file.source.state.FileSourceState;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConstants;
import org.apache.seatunnel.connectors.seatunnel.hive.source.config.HiveSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.source.config.MultipleTableHiveSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.source.reader.MultipleTableHiveSourceReader;
import org.apache.seatunnel.connectors.seatunnel.hive.source.split.MultipleTableHiveSourceSplitEnumerator;

import java.util.List;
import java.util.stream.Collectors;

public class HiveSource extends BaseHdfsFileSource {

    private final MultipleTableHiveSourceConfig multipleTableHiveSourceConfig;

    public HiveSource(ReadonlyConfig readonlyConfig) {
        this.multipleTableHiveSourceConfig = new MultipleTableHiveSourceConfig(readonlyConfig);
    }

    @Override
    public String getPluginName() {
        return HiveConstants.CONNECTOR_NAME;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return multipleTableHiveSourceConfig.getHiveSourceConfigs().stream()
                .map(HiveSourceConfig::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public SourceReader<SeaTunnelRow, FileSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new MultipleTableHiveSourceReader(readerContext, multipleTableHiveSourceConfig);
    }

    @Override
    public SourceSplitEnumerator<FileSourceSplit, FileSourceState> createEnumerator(
            SourceSplitEnumerator.Context<FileSourceSplit> enumeratorContext) {
        return new MultipleTableHiveSourceSplitEnumerator(
                enumeratorContext, multipleTableHiveSourceConfig);
    }

    @Override
    public SourceSplitEnumerator<FileSourceSplit, FileSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<FileSourceSplit> enumeratorContext,
            FileSourceState checkpointState) {
        return new MultipleTableHiveSourceSplitEnumerator(
                enumeratorContext, multipleTableHiveSourceConfig, checkpointState);
    }
}

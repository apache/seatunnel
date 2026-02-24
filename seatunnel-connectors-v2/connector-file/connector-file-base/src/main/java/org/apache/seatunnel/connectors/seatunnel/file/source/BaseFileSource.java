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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.file.source.state.FileSourceState;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public abstract class BaseFileSource
        implements SeaTunnelSource<SeaTunnelRow, FileSourceSplit, FileSourceState>,
                SupportParallelism,
                SupportColumnProjection {
    protected CatalogTable catalogTable;
    protected ReadStrategy readStrategy;
    protected HadoopConf hadoopConf;
    protected List<String> filePaths;

    public BaseFileSource() {}

    public BaseFileSource(ReadonlyConfig pluginConfig) {
        String path = pluginConfig.get(FileBaseSinkOptions.FILE_PATH);
        readStrategy = pluginConfig.get(FileBaseSinkOptions.FILE_FORMAT_TYPE).getReadStrategy();
        readStrategy.setPluginConfig(pluginConfig.toConfig());
        readStrategy.init(hadoopConf);
        try {
            filePaths = readStrategy.getFileNamesByPath(path);
        } catch (IOException e) {
            String errorMsg = String.format("Get file list from this path [%s] failed", path);
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_LIST_GET_FAILED, errorMsg, e);
        }

        // support user-defined schema
        FileFormat fileFormat = pluginConfig.get(FileBaseSourceOptions.FILE_FORMAT_TYPE);
        // only json text csv type support user-defined schema now
        if (pluginConfig.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
            switch (fileFormat) {
                case CSV:
                case TEXT:
                case JSON:
                case EXCEL:
                case XML:
                    CatalogTable userDefinedCatalogTable =
                            CatalogTableUtil.buildWithConfig(pluginConfig);
                    readStrategy.setCatalogTable(userDefinedCatalogTable);
                    catalogTable = userDefinedCatalogTable;
                    break;
                case ORC:
                case PARQUET:
                case BINARY:
                    throw new FileConnectorException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                            "SeaTunnel does not support user-defined schema for [parquet, orc, binary] files");
                default:
                    // never got in there
                    throw new FileConnectorException(
                            CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                            "SeaTunnel does not supported this file format");
            }
        } else {
            if (filePaths.isEmpty()) {
                // When the directory is empty, distribute default behavior schema
                catalogTable = CatalogTableUtil.buildSimpleTextTable();
                return;
            }
            try {
                SeaTunnelRowType seaTunnelRowTypeInfo =
                        readStrategy.getSeaTunnelRowTypeInfo(filePaths.get(0));
                catalogTable =
                        CatalogTableUtil.getCatalogTable(
                                "schema", "default", null, "default", seaTunnelRowTypeInfo);
            } catch (FileConnectorException e) {
                String errorMsg =
                        String.format("Get table schema from file [%s] failed", filePaths.get(0));
                throw new FileConnectorException(
                        CommonErrorCodeDeprecated.TABLE_SCHEMA_GET_FAILED, errorMsg, e);
            }
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public SourceReader<SeaTunnelRow, FileSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new BaseFileSourceReader(readStrategy, readerContext);
    }

    @Override
    public SourceSplitEnumerator<FileSourceSplit, FileSourceState> createEnumerator(
            SourceSplitEnumerator.Context<FileSourceSplit> enumeratorContext) throws Exception {
        return new FileSourceSplitEnumerator(enumeratorContext, filePaths);
    }

    @Override
    public SourceSplitEnumerator<FileSourceSplit, FileSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<FileSourceSplit> enumeratorContext,
            FileSourceState checkpointState)
            throws Exception {
        return new FileSourceSplitEnumerator(enumeratorContext, filePaths, checkpointState);
    }
}

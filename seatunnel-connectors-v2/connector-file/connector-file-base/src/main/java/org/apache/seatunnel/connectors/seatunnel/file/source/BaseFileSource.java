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

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

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
    protected ReadonlyConfig pluginConfig;
    private final CatalogTable catalogTable;
    private final List<String> filePaths;
    private final ReadStrategy readStrategy;

    /** shouldn't use this construct method. just for testing */
    @VisibleForTesting
    public BaseFileSource() {
        this.catalogTable = null;
        this.filePaths = null;
        this.readStrategy = null;
    }

    public BaseFileSource(ReadonlyConfig pluginConfig) {
        this.pluginConfig = pluginConfig;
        HadoopConf hadoopConf = initHadoopConf();
        this.readStrategy =
                pluginConfig.get(FileBaseSourceOptions.FILE_FORMAT_TYPE).getReadStrategy();
        this.readStrategy.setPluginConfig(pluginConfig.toConfig());
        this.readStrategy.init(hadoopConf);
        String path = pluginConfig.get(FileBaseSourceOptions.FILE_PATH);
        try {
            filePaths = readStrategy.getFileNamesByPath(path);
        } catch (IOException e) {
            String errorMsg = String.format("Get file list from this path [%s] failed", path);
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_LIST_GET_FAILED, errorMsg, e);
        }

        // support user-defined schema
        CatalogTable userDefinedCatalogTable;
        FileFormat fileFormat = pluginConfig.get(FileBaseSourceOptions.FILE_FORMAT_TYPE);
        // only json text csv type support user-defined schema now
        if (pluginConfig.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
            switch (fileFormat) {
                case CSV:
                case TEXT:
                case JSON:
                case EXCEL:
                case XML:
                    userDefinedCatalogTable = CatalogTableUtil.buildWithConfig(pluginConfig);
                    readStrategy.setCatalogTable(userDefinedCatalogTable);
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
                userDefinedCatalogTable = buildCatalogTableForEmptyPath(path, fileFormat);
            } else {
                try {
                    SeaTunnelRowType rowType =
                            readStrategy.getSeaTunnelRowTypeInfo(filePaths.get(0));
                    userDefinedCatalogTable = CatalogTableUtil.getCatalogTable("default", rowType);
                } catch (FileConnectorException e) {
                    String errorMsg =
                            String.format(
                                    "Get table schema from file [%s] failed", filePaths.get(0));
                    throw new FileConnectorException(
                            CommonErrorCodeDeprecated.TABLE_SCHEMA_GET_FAILED, errorMsg, e);
                }
            }
        }
        this.catalogTable = userDefinedCatalogTable;
    }

    private CatalogTable buildCatalogTableForEmptyPath(String path, FileFormat fileFormat) {
        if (fileFormat != FileFormat.BINARY && fileFormat != FileFormat.MARKDOWN) {
            // Preserve the legacy simple-text fallback for formats that still infer schema from
            // the first concrete file.
            return CatalogTableUtil.buildSimpleTextTable();
        }
        SeaTunnelRowType rowType = readStrategy.getSeaTunnelRowTypeInfo(path);
        return CatalogTableUtil.getCatalogTable("default", rowType);
    }

    protected abstract HadoopConf initHadoopConf();

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

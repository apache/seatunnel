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

package org.apache.seatunnel.connectors.seatunnel.file.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.MarkdownKnowledgeSyncMetadata;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategyFactory;

import org.apache.commons.collections4.CollectionUtils;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Getter
@Slf4j
public abstract class BaseFileSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final CatalogTable catalogTable;
    private final FileFormat fileFormat;
    private final ReadStrategy readStrategy;
    private final List<String> filePaths;
    private final ReadonlyConfig baseFileSourceConfig;
    private final CatalogTable catalogTableFromConfig;
    private final boolean fileDiscoveryDeferred;
    /** Credential-safe root path rendered in file-discovery logs and exceptions. */
    @Getter(AccessLevel.NONE)
    private final String safeDiscoveryRootContext;

    public abstract HadoopConf getHadoopConfig();

    public abstract String getPluginName();

    public BaseFileSourceConfig(
            ReadonlyConfig readonlyConfig, CatalogTable catalogTableFromConfig) {
        this.baseFileSourceConfig = readonlyConfig;
        this.fileFormat = readonlyConfig.get(FileBaseSourceOptions.FILE_FORMAT_TYPE);
        this.readStrategy = ReadStrategyFactory.of(readonlyConfig, getHadoopConfig());
        this.fileDiscoveryDeferred = shouldDeferFileDiscovery(readonlyConfig);
        String rootPath = readonlyConfig.get(FileBaseSourceOptions.FILE_PATH);
        // Fail fast and retain the sanitized root for diagnostics without replacing the logical
        // identity that MarkdownReadStrategy derives for each discovered file.
        this.safeDiscoveryRootContext =
                isMarkdownKnowledgeSyncMetadataEnabled(readonlyConfig)
                        ? MarkdownKnowledgeSyncMetadata.canonicalizeSourceUri(rootPath)
                        : maskUriUserInfo(rootPath);
        this.filePaths = parseFilePaths(readonlyConfig);
        this.catalogTableFromConfig = catalogTableFromConfig;
        CatalogTable parsedCatalogTable = parseCatalogTable(readonlyConfig);
        this.catalogTable =
                isMarkdownKnowledgeSyncMetadataEnabled(readonlyConfig)
                        ? MarkdownKnowledgeSyncMetadata.withMetadata(
                                CatalogTable.withMetadata(
                                        parsedCatalogTable,
                                        catalogTableFromConfig.getMetadataSchema()))
                        : parsedCatalogTable;
    }

    protected boolean shouldDeferFileDiscovery(ReadonlyConfig readonlyConfig) {
        return false;
    }

    private List<String> parseFilePaths(ReadonlyConfig readonlyConfig) {
        if (readonlyConfig.get(FileBaseSourceOptions.DISCOVERY_MODE) == FileDiscoveryMode.CONTINUOUS
                || fileDiscoveryDeferred) {
            return Collections.emptyList();
        }
        return discoverFilePaths();
    }

    public List<String> getFilePathsForSplitEnumerator() {
        if (fileDiscoveryDeferred) {
            ReadStrategy discoveryReadStrategy =
                    ReadStrategyFactory.of(baseFileSourceConfig, getHadoopConfig());
            try {
                return discoverFilePaths(discoveryReadStrategy);
            } finally {
                closeDiscoveryReadStrategy(discoveryReadStrategy);
            }
        }
        return filePaths;
    }

    private void closeDiscoveryReadStrategy(ReadStrategy discoveryReadStrategy) {
        try {
            discoveryReadStrategy.close();
        } catch (IOException e) {
            log.warn("Failed to close file discovery resources for plugin {}", getPluginName(), e);
        }
    }

    private List<String> discoverFilePaths() {
        return discoverFilePaths(readStrategy);
    }

    private List<String> discoverFilePaths(ReadStrategy discoveryReadStrategy) {
        String rootPath = baseFileSourceConfig.get(FileBaseSourceOptions.FILE_PATH);
        long startTime = System.currentTimeMillis();
        try {
            List<String> discoveredFilePaths = discoveryReadStrategy.getFileNamesByPath(rootPath);
            log.info(
                    "File source discovery finished: plugin={}, path={}, files={}, cost={}ms",
                    getPluginName(),
                    safeDiscoveryRootContext,
                    discoveredFilePaths.size(),
                    System.currentTimeMillis() - startTime);
            return discoveredFilePaths;
        } catch (Exception ex) {
            String errorMsg =
                    String.format(
                            "Get file list from this path [%s] failed", safeDiscoveryRootContext);
            if (isMarkdownKnowledgeSyncMetadataEnabled(baseFileSourceConfig)) {
                throw new FileConnectorException(
                        FileConnectorErrorCode.FILE_LIST_GET_FAILED,
                        errorMsg,
                        MarkdownKnowledgeSyncMetadata.copyStackTraceOnly(ex));
            }
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_LIST_GET_FAILED, errorMsg, ex);
        }
    }

    private CatalogTable parseCatalogTable(ReadonlyConfig readonlyConfig) {
        final CatalogTable catalogTable = catalogTableFromConfig;
        boolean configSchema =
                readonlyConfig.getOptional(ConnectorCommonOptions.SCHEMA).isPresent();
        if (CollectionUtils.isEmpty(filePaths)) {
            // When there are no files (including sync_mode=update filtered all files), choose a
            // compatible schema so that downstream can initialize correctly.
            if (fileFormat == FileFormat.BINARY
                    || fileFormat == FileFormat.MARKDOWN
                    || fileFormat == FileFormat.PDF) {
                return newCatalogTable(catalogTable, getSchemaForEmptyFilePath(readonlyConfig));
            }
            return catalogTable;
        }
        switch (fileFormat) {
            case CSV:
            case TEXT:
            case JSON:
            case EXCEL:
            case XML:
                readStrategy.setCatalogTable(catalogTable);
                return newCatalogTable(catalogTable, readStrategy.getActualSeaTunnelRowTypeInfo());
            case ORC:
            case PARQUET:
            case BINARY:
                return newCatalogTable(
                        catalogTable,
                        readStrategy.getSeaTunnelRowTypeInfoWithUserConfigRowType(
                                filePaths.get(0),
                                configSchema ? catalogTable.getSeaTunnelRowType() : null));
            case MARKDOWN:
            case PDF:
                return newCatalogTable(
                        catalogTable, readStrategy.getSeaTunnelRowTypeInfo(filePaths.get(0)));
            default:
                throw new FileConnectorException(
                        FileConnectorErrorCode.FORMAT_NOT_SUPPORT,
                        "SeaTunnel does not supported this file format: [" + fileFormat + "]");
        }
    }

    /** Returns whether Markdown document routing and Knowledge Sync metadata are enabled. */
    private boolean isMarkdownKnowledgeSyncMetadataEnabled(ReadonlyConfig readonlyConfig) {
        return fileFormat == FileFormat.MARKDOWN
                && readonlyConfig.get(FileBaseSourceOptions.MARKDOWN_RAG_METADATA_ENABLED);
    }

    private SeaTunnelRowType getSchemaForEmptyFilePath(ReadonlyConfig readonlyConfig) {
        String rootPath = readonlyConfig.get(FileBaseSourceOptions.FILE_PATH);
        return readStrategy.getSeaTunnelRowTypeInfo(rootPath);
    }

    private static String maskUriUserInfo(String rawPath) {
        if (rawPath == null) {
            return null;
        }
        try {
            URI uri = URI.create(rawPath);
            if (uri.getUserInfo() == null || uri.getAuthority() == null) {
                return rawPath;
            }
            String maskedAuthority = uri.getAuthority().replace(uri.getUserInfo() + "@", "***@");
            return new URI(
                            uri.getScheme(),
                            maskedAuthority,
                            uri.getPath(),
                            uri.getQuery(),
                            uri.getFragment())
                    .toString();
        } catch (Exception e) {
            return rawPath;
        }
    }

    private CatalogTable newCatalogTable(
            CatalogTable catalogTable, SeaTunnelRowType seaTunnelRowType) {
        TableSchema tableSchema = catalogTable.getTableSchema();

        Map<String, Column> columnMap =
                tableSchema.getColumns().stream()
                        .collect(Collectors.toMap(Column::getName, Function.identity()));
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();

        List<Column> finalColumns = new ArrayList<>();
        for (int i = 0; i < fieldNames.length; i++) {
            Column column = columnMap.get(fieldNames[i]);
            if (column != null) {
                finalColumns.add(column);
            } else {
                finalColumns.add(
                        PhysicalColumn.of(fieldNames[i], fieldTypes[i], 0, false, null, null));
            }
        }

        TableSchema finalSchema =
                TableSchema.builder()
                        .columns(finalColumns)
                        .primaryKey(tableSchema.getPrimaryKey())
                        .constraintKey(tableSchema.getConstraintKeys())
                        .build();

        return CatalogTable.of(
                catalogTable.getTableId(),
                finalSchema,
                catalogTable.getOptions(),
                catalogTable.getPartitionKeys(),
                catalogTable.getComment(),
                catalogTable.getCatalogName());
    }
}

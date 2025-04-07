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
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategyFactory;

import org.apache.commons.collections4.CollectionUtils;

import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Getter
public abstract class BaseFileSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final CatalogTable catalogTable;
    private final FileFormat fileFormat;
    private final ReadStrategy readStrategy;
    private final List<String> filePaths;
    private final ReadonlyConfig baseFileSourceConfig;

    public abstract HadoopConf getHadoopConfig();

    public abstract String getPluginName();

    public BaseFileSourceConfig(ReadonlyConfig readonlyConfig) {
        this.baseFileSourceConfig = readonlyConfig;
        this.fileFormat = readonlyConfig.get(BaseSourceConfigOptions.FILE_FORMAT_TYPE);
        this.readStrategy = ReadStrategyFactory.of(readonlyConfig, getHadoopConfig());
        this.filePaths = parseFilePaths(readonlyConfig);

        this.catalogTable = parseCatalogTable(readonlyConfig);
    }

    private List<String> parseFilePaths(ReadonlyConfig readonlyConfig) {
        String rootPath = null;
        try {
            rootPath = readonlyConfig.get(BaseSourceConfigOptions.FILE_PATH);
            return readStrategy.getFileNamesByPath(rootPath);
        } catch (Exception ex) {
            String errorMsg = String.format("Get file list from this path [%s] failed", rootPath);
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_LIST_GET_FAILED, errorMsg, ex);
        }
    }

    private CatalogTable parseCatalogTable(ReadonlyConfig readonlyConfig) {
        final CatalogTable catalogTable;
        boolean configSchema =
                readonlyConfig.getOptional(ConnectorCommonOptions.SCHEMA).isPresent();
        if (configSchema) {
            catalogTable = CatalogTableUtil.buildWithConfig(getPluginName(), readonlyConfig);
        } else {
            catalogTable = CatalogTableUtil.buildSimpleTextTable();
        }
        if (CollectionUtils.isEmpty(filePaths)) {
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
            default:
                throw new FileConnectorException(
                        FileConnectorErrorCode.FORMAT_NOT_SUPPORT,
                        "SeaTunnel does not supported this file format: [" + fileFormat + "]");
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

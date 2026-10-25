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

package org.apache.seatunnel.connectors.seatunnel.hive.source.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategyFactory;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConstants;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hive.storage.StorageFactory;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveTableUtils;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveTypeConvertor;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import lombok.Getter;
import lombok.SneakyThrows;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSinkOptions.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSinkOptions.FILE_FORMAT_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSinkOptions.ROW_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions.NULL_FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions.PARSE_PARTITION_FROM_PATH;
import static org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions.READ_COLUMNS;

@Getter
public class HiveSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final CatalogTable catalogTable;
    private final FileFormat fileFormat;
    private final ReadStrategy readStrategy;
    private final List<String> filePaths;
    private final HadoopConf hadoopConf;

    @SneakyThrows
    public HiveSourceConfig(ReadonlyConfig readonlyConfig) {
        readonlyConfig
                .getOptional(HiveSourceOptions.READ_PARTITIONS)
                .ifPresent(this::validatePartitions);
        Table table;
        try {
            table = HiveTableUtils.getTableInfo(readonlyConfig);
        } catch (Exception e) {
            String tableName =
                    readonlyConfig.getOptional(HiveSourceOptions.TABLE_NAME).orElse("<missing>");
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HIVE_TABLE_INFORMATION_FAILED,
                    "Failed to get Hive table information for table_name='"
                            + tableName
                            + "'. Please ensure metastore is reachable and the table exists.",
                    e);
        }
        this.hadoopConf = parseHiveHadoopConfig(readonlyConfig, table);
        this.fileFormat = HiveTableUtils.parseFileFormat(table);
        this.readStrategy = parseReadStrategy(table, readonlyConfig, fileFormat, hadoopConf);
        this.filePaths = parseFilePaths(table, readStrategy);
        this.catalogTable =
                parseCatalogTable(
                        readonlyConfig, readStrategy, fileFormat, hadoopConf, filePaths, table);
    }

    private void validatePartitions(List<String> partitionsList) {
        if (CollectionUtils.isEmpty(partitionsList)) {
            throw new HiveConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "Partitions list is empty, please check");
        }
        int depth = partitionsList.get(0).replaceAll("\\\\", "/").split("/").length;
        long count =
                partitionsList.stream()
                        .map(partition -> partition.replaceAll("\\\\", "/").split("/").length)
                        .filter(length -> length != depth)
                        .count();
        if (count > 0) {
            throw new HiveConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    "Every partition that in partition list should has the same directory depth");
        }
    }

    private ReadStrategy parseReadStrategy(
            Table table,
            ReadonlyConfig readonlyConfig,
            FileFormat fileFormat,
            HadoopConf hadoopConf) {

        ReadStrategy readStrategy = ReadStrategyFactory.of(fileFormat.name());
        Config config = readonlyConfig.toConfig();

        switch (fileFormat) {
            case TEXT:
                // if the file format is text, we set the delim.
                Map<String, String> parameters = table.getSd().getSerdeInfo().getParameters();
                if (!readonlyConfig.getOptional(NULL_FORMAT).isPresent()) {
                    String nullFormatKey = "serialization.null.format";
                    String nullFormat = table.getParameters().get(nullFormatKey);
                    if (StringUtils.isEmpty(nullFormat)) {
                        nullFormat = parameters.get(nullFormatKey);
                    }
                    if (StringUtils.isEmpty(nullFormat)) {
                        nullFormat = "\\N";
                    }
                    config =
                            config.withValue(
                                    NULL_FORMAT.key(), ConfigValueFactory.fromAnyRef(nullFormat));
                }
                config =
                        config.withValue(
                                        FIELD_DELIMITER.key(),
                                        ConfigValueFactory.fromAnyRef(
                                                parameters.get("field.delim")))
                                .withValue(
                                        ROW_DELIMITER.key(),
                                        ConfigValueFactory.fromAnyRef(parameters.get("line.delim")))
                                .withValue(
                                        FILE_FORMAT_TYPE.key(),
                                        ConfigValueFactory.fromAnyRef(FileFormat.TEXT.name()));
                break;
            case ORC:
                config =
                        config.withValue(
                                FILE_FORMAT_TYPE.key(),
                                ConfigValueFactory.fromAnyRef(FileFormat.ORC.name()));
                break;
            case PARQUET:
                config =
                        config.withValue(
                                FILE_FORMAT_TYPE.key(),
                                ConfigValueFactory.fromAnyRef(FileFormat.PARQUET.name()));
                break;
            default:
        }
        readStrategy.setPluginConfig(config);
        readStrategy.init(hadoopConf);
        return readStrategy;
    }

    private HadoopConf parseHiveHadoopConfig(ReadonlyConfig readonlyConfig, Table table) {
        String hiveSdLocation = table.getSd().getLocation();
        /**
         * Build hadoop conf(support s3、cos、oss、hdfs). The returned hadoop conf can be
         * CosConf、OssConf、S3Conf、HadoopConf so that HadoopFileSystemProxy can obtain the correct
         * Schema and FsHdfsImpl that can be filled into hadoop configuration in {@link
         * org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy#createConfiguration()}
         */
        HadoopConf hadoopConf =
                StorageFactory.getStorageType(hiveSdLocation)
                        .buildHadoopConfWithReadOnlyConfig(readonlyConfig);
        readonlyConfig
                .getOptional(HiveSourceOptions.HDFS_SITE_PATH)
                .ifPresent(hadoopConf::setHdfsSitePath);
        readonlyConfig.getOptional(HiveSourceOptions.KRB5_PATH).ifPresent(hadoopConf::setKrb5Path);
        readonlyConfig
                .getOptional(HiveSourceOptions.KERBEROS_PRINCIPAL)
                .ifPresent(hadoopConf::setKerberosPrincipal);
        readonlyConfig
                .getOptional(HiveSourceOptions.KERBEROS_KEYTAB_PATH)
                .ifPresent(hadoopConf::setKerberosKeytabPath);
        readonlyConfig
                .getOptional(HiveSourceOptions.REMOTE_USER)
                .ifPresent(hadoopConf::setRemoteUser);
        return hadoopConf;
    }

    private List<String> parseFilePaths(Table table, ReadStrategy readStrategy) {
        String hdfsPath = parseHdfsPath(table);
        try {
            return readStrategy.getFileNamesByPath(hdfsPath);
        } catch (IOException e) {
            if (isFileNotFound(e)) {
                return Collections.emptyList();
            }
            String errorMsg =
                    String.format(
                            "Get file list from this path [%s] failed, caused by: %s",
                            hdfsPath, getExceptionSummary(e));
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_LIST_GET_FAILED, errorMsg, e);
        }
    }

    private static String getExceptionSummary(Throwable throwable) {
        String message = throwable.getMessage();
        if (StringUtils.isBlank(message)) {
            return throwable.getClass().getName();
        }
        return throwable.getClass().getName() + ": " + message;
    }

    private static boolean isFileNotFound(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof FileNotFoundException
                    || current instanceof NoSuchFileException
                    || current instanceof PathNotFoundException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private String parseFsDefaultName(Table table) {
        String hdfsLocation = table.getSd().getLocation();
        try {
            URI uri = new URI(hdfsLocation);
            String path = uri.getPath();
            return hdfsLocation.replace(path, "");
        } catch (URISyntaxException e) {
            String errorMsg =
                    String.format(
                            "Get hdfs namenode host from table location [%s] failed,"
                                    + "please check it",
                            hdfsLocation);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HDFS_NAMENODE_HOST_FAILED, errorMsg, e);
        }
    }

    private String parseHdfsPath(Table table) {
        String hdfsLocation = table.getSd().getLocation();
        try {
            URI uri = new URI(hdfsLocation);
            return uri.getPath();
        } catch (URISyntaxException e) {
            String errorMsg =
                    String.format(
                            "Get hdfs namenode host from table location [%s] failed,"
                                    + "please check it",
                            hdfsLocation);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HDFS_NAMENODE_HOST_FAILED, errorMsg, e);
        }
    }

    private CatalogTable parseCatalogTable(
            ReadonlyConfig readonlyConfig,
            ReadStrategy readStrategy,
            FileFormat fileFormat,
            HadoopConf hadoopConf,
            List<String> filePaths,
            Table table) {
        if (CollectionUtils.isEmpty(filePaths)) {
            return handleEmptyFilesFallback(readonlyConfig, table);
        }
        switch (fileFormat) {
            case PARQUET:
            case ORC:
                return parseCatalogTableFromRemotePath(
                        readonlyConfig, hadoopConf, filePaths, table);
            case TEXT:
                return parseCatalogTableFromTable(readonlyConfig, readStrategy, table);
            default:
                throw new HiveConnectorException(
                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                        "Hive connector only support [text parquet orc] table now");
        }
    }

    private static CatalogTable handleEmptyFilesFallback(
            ReadonlyConfig readonlyConfig, Table table) {
        // Keep a stable schema even when directory is empty.
        return buildCatalogTableFromHiveMeta(readonlyConfig, table);
    }

    private CatalogTable parseCatalogTableFromRemotePath(
            ReadonlyConfig readonlyConfig,
            HadoopConf hadoopConf,
            List<String> filePaths,
            Table table) {
        CatalogTable catalogTable = buildEmptyCatalogTable(readonlyConfig, table);
        try {
            SeaTunnelRowType seaTunnelRowTypeInfo =
                    readStrategy.getSeaTunnelRowTypeInfo(filePaths.get(0));
            return CatalogTableUtil.newCatalogTable(catalogTable, seaTunnelRowTypeInfo);
        } catch (FileConnectorException e) {
            String errorMsg =
                    String.format("Get table schema from file [%s] failed", filePaths.get(0));
            throw new FileConnectorException(
                    CommonErrorCodeDeprecated.TABLE_SCHEMA_GET_FAILED, errorMsg, e);
        }
    }

    private CatalogTable parseCatalogTableFromTable(
            ReadonlyConfig readonlyConfig, ReadStrategy readStrategy, Table table) {
        SeaTunnelRowType seaTunnelRowType = buildRowTypeFromHiveMeta(table);
        readStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable(
                        "hive", table.getDbName(), null, table.getTableName(), seaTunnelRowType));
        final SeaTunnelRowType finalSeatunnelRowType = readStrategy.getActualSeaTunnelRowTypeInfo();

        CatalogTable catalogTable = buildEmptyCatalogTable(readonlyConfig, table);
        return CatalogTableUtil.newCatalogTable(catalogTable, finalSeatunnelRowType);
    }

    /**
     * Build a {@link CatalogTable} based on Hive metastore schema (table columns + optional
     * partition columns). This is used as a fallback when there are no data files to infer schema
     * from.
     */
    static CatalogTable buildCatalogTableFromHiveMeta(ReadonlyConfig readonlyConfig, Table table) {
        SeaTunnelRowType rowType = buildRowTypeFromHiveMeta(table);
        rowType = applyColumnProjectionIfPresent(readonlyConfig, rowType);
        if (shouldParsePartitionFromPath(readonlyConfig)) {
            rowType = appendPartitionColumnsAsString(table, rowType);
        }
        return CatalogTableUtil.newCatalogTable(
                buildEmptyCatalogTable(readonlyConfig, table), rowType);
    }

    private static SeaTunnelRowType buildRowTypeFromHiveMeta(Table table) {
        List<FieldSchema> cols = table.getSd().getCols();
        String[] fieldNames = new String[cols.size()];
        SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType[cols.size()];
        for (int i = 0; i < cols.size(); i++) {
            FieldSchema col = cols.get(i);
            fieldNames[i] = col.getName();
            fieldTypes[i] =
                    HiveTypeConvertor.covertHiveTypeToSeaTunnelType(col.getName(), col.getType());
        }
        return new SeaTunnelRowType(fieldNames, fieldTypes);
    }

    private static SeaTunnelRowType applyColumnProjectionIfPresent(
            ReadonlyConfig readonlyConfig, SeaTunnelRowType rowType) {
        List<String> readColumns = readonlyConfig.getOptional(READ_COLUMNS).orElse(null);
        if (CollectionUtils.isEmpty(readColumns)) {
            return rowType;
        }
        String[] fieldNames = new String[readColumns.size()];
        SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType[readColumns.size()];
        for (int i = 0; i < readColumns.size(); i++) {
            String colName = readColumns.get(i);
            int index = rowType.indexOf(colName, false);
            if (index < 0) {
                throw new HiveConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        String.format("read_columns contains non-existent column '%s'", colName));
            }
            fieldNames[i] = rowType.getFieldName(index);
            fieldTypes[i] = rowType.getFieldType(index);
        }
        return new SeaTunnelRowType(fieldNames, fieldTypes);
    }

    private static boolean shouldParsePartitionFromPath(ReadonlyConfig readonlyConfig) {
        return readonlyConfig
                .getOptional(PARSE_PARTITION_FROM_PATH)
                .orElse(PARSE_PARTITION_FROM_PATH.defaultValue());
    }

    private static SeaTunnelRowType appendPartitionColumnsAsString(
            Table table, SeaTunnelRowType rowType) {
        List<String> partitionKeys = extractPartitionKeyNames(table);
        if (CollectionUtils.isEmpty(partitionKeys)) {
            return rowType;
        }
        String[] baseFieldNames = rowType.getFieldNames();
        SeaTunnelDataType<?>[] baseFieldTypes = rowType.getFieldTypes();
        String[] newFieldNames =
                Arrays.copyOf(baseFieldNames, baseFieldNames.length + partitionKeys.size());
        SeaTunnelDataType<?>[] newFieldTypes =
                Arrays.copyOf(baseFieldTypes, baseFieldTypes.length + partitionKeys.size());
        int offset = baseFieldNames.length;
        for (int i = 0; i < partitionKeys.size(); i++) {
            newFieldNames[offset + i] = partitionKeys.get(i);
            newFieldTypes[offset + i] = BasicType.STRING_TYPE;
        }
        return new SeaTunnelRowType(newFieldNames, newFieldTypes);
    }

    private static List<String> extractPartitionKeyNames(Table table) {
        List<FieldSchema> partitionKeys = table.getPartitionKeys();
        if (CollectionUtils.isEmpty(partitionKeys)) {
            return new ArrayList<>();
        }
        List<String> names = new ArrayList<>(partitionKeys.size());
        for (FieldSchema key : partitionKeys) {
            if (key != null && StringUtils.isNotBlank(key.getName())) {
                names.add(key.getName());
            }
        }
        return names;
    }

    private static CatalogTable buildEmptyCatalogTable(ReadonlyConfig readonlyConfig, Table table) {
        TablePath tablePath = TablePath.of(table.getDbName(), table.getTableName());
        return CatalogTable.of(
                TableIdentifier.of(HiveConstants.CONNECTOR_NAME, tablePath),
                TableSchema.builder().build(),
                new HashMap<>(),
                extractPartitionKeyNames(table),
                readonlyConfig.get(ConnectorCommonOptions.TABLE_COMMENT));
    }
}

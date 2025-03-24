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

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.config.HdfsSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategyFactory;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConstants;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hive.storage.StorageFactory;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveTableUtils;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveTypeConvertor;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import lombok.Getter;
import lombok.SneakyThrows;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.FILE_FORMAT_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.ROW_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions.NULL_FORMAT;

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
                .getOptional(HdfsSourceConfigOptions.READ_PARTITIONS)
                .ifPresent(this::validatePartitions);
        Table table = HiveTableUtils.getTableInfo(readonlyConfig);
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
                .getOptional(HdfsSourceConfigOptions.HDFS_SITE_PATH)
                .ifPresent(hadoopConf::setHdfsSitePath);
        readonlyConfig
                .getOptional(HdfsSourceConfigOptions.KRB5_PATH)
                .ifPresent(hadoopConf::setKrb5Path);
        readonlyConfig
                .getOptional(HdfsSourceConfigOptions.KERBEROS_PRINCIPAL)
                .ifPresent(hadoopConf::setKerberosPrincipal);
        readonlyConfig
                .getOptional(HdfsSourceConfigOptions.KERBEROS_KEYTAB_PATH)
                .ifPresent(hadoopConf::setKerberosKeytabPath);
        readonlyConfig
                .getOptional(HdfsSourceConfigOptions.REMOTE_USER)
                .ifPresent(hadoopConf::setRemoteUser);
        return hadoopConf;
    }

    private List<String> parseFilePaths(Table table, ReadStrategy readStrategy) {
        String hdfsPath = parseHdfsPath(table);
        try {
            return readStrategy.getFileNamesByPath(hdfsPath);
        } catch (Exception e) {
            String errorMsg = String.format("Get file list from this path [%s] failed", hdfsPath);
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_LIST_GET_FAILED, errorMsg, e);
        }
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

    private CatalogTable parseCatalogTableFromRemotePath(
            ReadonlyConfig readonlyConfig,
            HadoopConf hadoopConf,
            List<String> filePaths,
            Table table) {
        if (CollectionUtils.isEmpty(filePaths)) {
            // When the directory is empty, distribute default behavior schema
            return buildEmptyCatalogTable(readonlyConfig, table);
        }
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
        List<FieldSchema> cols = table.getSd().getCols();
        String[] fieldNames = new String[cols.size()];
        SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType[cols.size()];
        for (int i = 0; i < cols.size(); i++) {
            FieldSchema col = cols.get(i);
            fieldNames[i] = col.getName();
            fieldTypes[i] =
                    HiveTypeConvertor.covertHiveTypeToSeaTunnelType(col.getName(), col.getType());
        }

        SeaTunnelRowType seaTunnelRowType = new SeaTunnelRowType(fieldNames, fieldTypes);
        readStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable(
                        "hive", table.getDbName(), null, table.getTableName(), seaTunnelRowType));
        final SeaTunnelRowType finalSeatunnelRowType = readStrategy.getActualSeaTunnelRowTypeInfo();

        CatalogTable catalogTable = buildEmptyCatalogTable(readonlyConfig, table);
        return CatalogTableUtil.newCatalogTable(catalogTable, finalSeatunnelRowType);
    }

    private CatalogTable buildEmptyCatalogTable(ReadonlyConfig readonlyConfig, Table table) {
        TablePath tablePath = TablePath.of(table.getDbName(), table.getTableName());
        return CatalogTable.of(
                TableIdentifier.of(HiveConstants.CONNECTOR_NAME, tablePath),
                TableSchema.builder().build(),
                new HashMap<>(),
                new ArrayList<>(),
                readonlyConfig.get(ConnectorCommonOptions.TABLE_COMMENT));
    }
}

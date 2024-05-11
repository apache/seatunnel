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
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.config.HdfsSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategyFactory;
import org.apache.seatunnel.connectors.seatunnel.hive.catalog.HiveTable;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hive.storage.StorageFactory;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveMetaStoreProxy;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.hive.metastore.api.Table;

import lombok.Getter;
import lombok.SneakyThrows;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.FILE_FORMAT_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.ROW_DELIMITER;

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
        String table = readonlyConfig.get(HiveConfig.TABLE_NAME);
        TablePath tablePath = TablePath.of(table);
        HiveTable tableInformation =
                HiveMetaStoreProxy.getInstance(readonlyConfig)
                        .getTableInfo(tablePath.getDatabaseName(), tablePath.getTableName());
        this.hadoopConf = parseHiveHadoopConfig(readonlyConfig, tableInformation.getLocation());
        this.fileFormat = tableInformation.getInputFormat();
        this.readStrategy =
                parseReadStrategy(
                        tableInformation.getTableParameters(),
                        readonlyConfig,
                        fileFormat,
                        hadoopConf);
        this.filePaths = parseFilePaths(tableInformation.getLocation(), readStrategy);
        this.catalogTable = tableInformation.getCatalogTable();
        SeaTunnelRowType seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        List<String> partitionKeys = catalogTable.getPartitionKeys();
        int size = catalogTable.getSeaTunnelRowType().getTotalFields() - partitionKeys.size();
        String[] fieldNames = new String[size];
        SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType[size];
        String[] totalFieldNames = seaTunnelRowType.getFieldNames();
        for (int i = 0; i < totalFieldNames.length; i++) {
            if (!partitionKeys.contains(totalFieldNames[i])) {
                fieldNames[i] = totalFieldNames[i];
                fieldTypes[i] = seaTunnelRowType.getFieldType(i);
            }
        }
        SeaTunnelRowType rowTypeWithoutPartitionField =
                new SeaTunnelRowType(fieldNames, fieldTypes);
        readStrategy.setSeaTunnelRowTypeInfo(rowTypeWithoutPartitionField);
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
            Map<String, String> parameters,
            ReadonlyConfig readonlyConfig,
            FileFormat fileFormat,
            HadoopConf hadoopConf) {

        ReadStrategy readStrategy = ReadStrategyFactory.of(fileFormat.name());
        Config config = readonlyConfig.toConfig();

        switch (fileFormat) {
            case TEXT:
                // if the file format is text, we set the delim.
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

    private HadoopConf parseHiveHadoopConfig(ReadonlyConfig readonlyConfig, String hiveSdLocation) {
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

    private List<String> parseFilePaths(String hdfsPath, ReadStrategy readStrategy) {
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
}

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

package org.apache.seatunnel.connectors.seatunnel.hive.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.WriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.WriteStrategyFactory;
import org.apache.seatunnel.connectors.seatunnel.hive.catalog.HiveJDBCCatalog;
import org.apache.seatunnel.connectors.seatunnel.hive.catalog.HiveTable;
import org.apache.seatunnel.connectors.seatunnel.hive.commit.HiveSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConstants;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hive.sink.writter.HiveSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.hive.storage.StorageFactory;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveMetaStoreProxy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.FILE_FORMAT_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.FILE_NAME_EXPRESSION;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.FILE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.IS_PARTITION_FIELD_WRITE_IN_FILE;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.PARTITION_BY;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.ROW_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.SINK_COLUMNS;

public class HiveSink
        implements SeaTunnelSink<
                        SeaTunnelRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo>,
                SupportSaveMode,
                SupportMultiTableSink {

    private final CatalogTable catalogTable;
    private final ReadonlyConfig readonlyConfig;
    private String jobId;

    public HiveSink(ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        this.readonlyConfig = readonlyConfig;
        this.catalogTable = catalogTable;
    }

    private HiveTable getHiveTable() {
        return HiveMetaStoreProxy.getInstance(readonlyConfig)
                .getTableInfo(
                        catalogTable.getTablePath().getDatabaseName(),
                        catalogTable.getTablePath().getTableName());
    }

    @Override
    public String getPluginName() {
        return HiveConstants.CONNECTOR_NAME;
    }

    @Override
    public Optional<SinkAggregatedCommitter<FileCommitInfo, FileAggregatedCommitInfo>>
            createAggregatedCommitter() {
        return Optional.of(
                new HiveSinkAggregatedCommitter(
                        readonlyConfig,
                        catalogTable.getTableId().getDatabaseName(),
                        catalogTable.getTableId().getTableName(),
                        createHadoopConf(readonlyConfig)));
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobId = jobContext.getJobId();
    }

    @Override
    public SinkWriter<SeaTunnelRow, FileCommitInfo, FileSinkState> restoreWriter(
            SinkWriter.Context context, List<FileSinkState> states) {
        return new HiveSinkWriter(
                getWriteStrategy(), createHadoopConf(readonlyConfig), context, jobId, states);
    }

    @Override
    public SinkWriter<SeaTunnelRow, FileCommitInfo, FileSinkState> createWriter(
            SinkWriter.Context context) {
        return new HiveSinkWriter(
                getWriteStrategy(), createHadoopConf(readonlyConfig), context, jobId);
    }

    @Override
    public Optional<Serializer<FileCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<Serializer<FileAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<Serializer<FileSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    private HadoopConf createHadoopConf(ReadonlyConfig readonlyConfig) {
        /**
         * Build hadoop conf(support s3、cos、oss、hdfs). The returned hadoop conf can be
         * CosConf、OssConf、S3Conf、HadoopConf so that HadoopFileSystemProxy can obtain the correct
         * Schema and FsHdfsImpl that can be filled into hadoop configuration in {@link
         * org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy#createConfiguration()}
         */
        HadoopConf hadoopConf =
                StorageFactory.getStorageType(getHiveTable().getLocation())
                        .buildHadoopConfWithReadOnlyConfig(readonlyConfig);
        readonlyConfig
                .getOptional(HiveSinkOptions.HDFS_SITE_PATH)
                .ifPresent(hadoopConf::setHdfsSitePath);
        readonlyConfig
                .getOptional(HiveSinkOptions.REMOTE_USER)
                .ifPresent(hadoopConf::setRemoteUser);
        readonlyConfig
                .getOptional(HiveSinkOptions.KERBEROS_PRINCIPAL)
                .ifPresent(hadoopConf::setKerberosPrincipal);
        readonlyConfig
                .getOptional(HiveSinkOptions.KERBEROS_KEYTAB_PATH)
                .ifPresent(hadoopConf::setKerberosKeytabPath);
        return hadoopConf;
    }

    private FileSinkConfig generateFileSinkConfig(ReadonlyConfig readonlyConfig) {
        HiveTable tableInformation = getHiveTable();
        CatalogTable hiveTable = tableInformation.getCatalogTable();
        Config pluginConfig = readonlyConfig.toConfig();
        List<String> sinkFields =
                hiveTable.getTableSchema().getColumns().stream()
                        .map(Column::getName)
                        .collect(Collectors.toList());
        switch (tableInformation.getInputFormat()) {
            case TEXT:
                Map<String, String> parameters = tableInformation.getTableParameters();
                pluginConfig =
                        pluginConfig
                                .withValue(
                                        FILE_FORMAT_TYPE.key(),
                                        ConfigValueFactory.fromAnyRef(FileFormat.TEXT.toString()))
                                .withValue(
                                        FIELD_DELIMITER.key(),
                                        ConfigValueFactory.fromAnyRef(
                                                parameters.get("field.delim")))
                                .withValue(
                                        ROW_DELIMITER.key(),
                                        ConfigValueFactory.fromAnyRef(
                                                parameters.get("line.delim")));
                break;
            case PARQUET:
                pluginConfig =
                        pluginConfig.withValue(
                                FILE_FORMAT_TYPE.key(),
                                ConfigValueFactory.fromAnyRef(FileFormat.PARQUET.toString()));
                break;
            case ORC:
                pluginConfig =
                        pluginConfig.withValue(
                                FILE_FORMAT_TYPE.key(),
                                ConfigValueFactory.fromAnyRef(FileFormat.ORC.toString()));
                break;
            default:
                throw new HiveConnectorException(
                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                        "Hive connector only support [text parquet orc] table now");
        }
        pluginConfig =
                pluginConfig
                        .withValue(
                                IS_PARTITION_FIELD_WRITE_IN_FILE.key(),
                                ConfigValueFactory.fromAnyRef(false))
                        .withValue(
                                FILE_NAME_EXPRESSION.key(),
                                ConfigValueFactory.fromAnyRef("${transactionId}"))
                        .withValue(
                                FILE_PATH.key(),
                                ConfigValueFactory.fromAnyRef(tableInformation.getLocation()))
                        .withValue(SINK_COLUMNS.key(), ConfigValueFactory.fromAnyRef(sinkFields))
                        .withValue(
                                PARTITION_BY.key(),
                                ConfigValueFactory.fromAnyRef(hiveTable.getPartitionKeys()));
        // use source catalog table data type.
        return new FileSinkConfig(pluginConfig, catalogTable.getSeaTunnelRowType());
    }

    private WriteStrategy getWriteStrategy() {
        FileSinkConfig fileSinkConfig = generateFileSinkConfig(readonlyConfig);
        WriteStrategy writeStrategy =
                WriteStrategyFactory.of(fileSinkConfig.getFileFormat(), fileSinkConfig);
        writeStrategy.setSeaTunnelRowTypeInfo(catalogTable.getSeaTunnelRowType());
        return writeStrategy;
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        if (!readonlyConfig.getOptional(HiveConfig.HIVE_JDBC_URL).isPresent()) {
            // if user not set jdbc url, skip save mode function
            return Optional.empty();
        }
        HiveJDBCCatalog catalog = new HiveJDBCCatalog(readonlyConfig);
        return Optional.of(
                new DefaultSaveModeHandler(
                        readonlyConfig.get(HiveSinkOptions.SCHEMA_SAVE_MODE),
                        readonlyConfig.get(HiveSinkOptions.DATA_SAVE_MODE),
                        catalog,
                        catalogTable,
                        ""));
    }
}

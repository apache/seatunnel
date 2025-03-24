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
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
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
import org.apache.seatunnel.connectors.seatunnel.hive.commit.HiveSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConstants;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveOptions;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hive.sink.writter.HiveSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.hive.storage.StorageFactory;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveTableUtils;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

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
                SupportMultiTableSink {

    // Since Table might contain some unserializable fields, we need to make it transient
    // And use getTableInformation to get the Table object
    private transient Table tableInformation;
    private final CatalogTable catalogTable;
    private final ReadonlyConfig readonlyConfig;
    private final HadoopConf hadoopConf;
    private final FileSinkConfig fileSinkConfig;
    private transient WriteStrategy writeStrategy;
    private String jobId;

    public HiveSink(ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        this.readonlyConfig = readonlyConfig;
        this.catalogTable = catalogTable;
        this.tableInformation = getTableInformation();
        this.hadoopConf = createHadoopConf(readonlyConfig);
        this.fileSinkConfig = generateFileSinkConfig(readonlyConfig, catalogTable);
        this.writeStrategy = getWriteStrategy();
    }

    private FileSinkConfig generateFileSinkConfig(
            ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        Table tableInformation = getTableInformation();
        Config pluginConfig = readonlyConfig.toConfig();
        List<String> sinkFields =
                tableInformation.getSd().getCols().stream()
                        .map(FieldSchema::getName)
                        .collect(Collectors.toList());
        List<String> partitionKeys =
                tableInformation.getPartitionKeys().stream()
                        .map(FieldSchema::getName)
                        .collect(Collectors.toList());
        sinkFields.addAll(partitionKeys);

        FileFormat fileFormat = HiveTableUtils.parseFileFormat(tableInformation);
        switch (fileFormat) {
            case TEXT:
                Map<String, String> parameters =
                        tableInformation.getSd().getSerdeInfo().getParameters();
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
                                ConfigValueFactory.fromAnyRef(
                                        tableInformation.getSd().getLocation()))
                        .withValue(SINK_COLUMNS.key(), ConfigValueFactory.fromAnyRef(sinkFields))
                        .withValue(
                                PARTITION_BY.key(), ConfigValueFactory.fromAnyRef(partitionKeys));

        return new FileSinkConfig(pluginConfig, catalogTable.getSeaTunnelRowType());
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
                        getTableInformation().getDbName(),
                        getTableInformation().getTableName(),
                        hadoopConf));
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobId = jobContext.getJobId();
    }

    @Override
    public HiveSinkWriter restoreWriter(SinkWriter.Context context, List<FileSinkState> states) {
        return new HiveSinkWriter(getWriteStrategy(), hadoopConf, context, jobId, states);
    }

    @Override
    public HiveSinkWriter createWriter(SinkWriter.Context context) {
        return new HiveSinkWriter(getWriteStrategy(), hadoopConf, context, jobId);
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
        String hdfsLocation = getTableInformation().getSd().getLocation();

        /**
         * Build hadoop conf(support s3、cos、oss、hdfs). The returned hadoop conf can be
         * CosConf、OssConf、S3Conf、HadoopConf so that HadoopFileSystemProxy can obtain the correct
         * Schema and FsHdfsImpl that can be filled into hadoop configuration in {@link
         * org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy#createConfiguration()}
         */
        HadoopConf hadoopConf =
                StorageFactory.getStorageType(hdfsLocation)
                        .buildHadoopConfWithReadOnlyConfig(readonlyConfig);
        readonlyConfig
                .getOptional(HiveOptions.HDFS_SITE_PATH)
                .ifPresent(hadoopConf::setHdfsSitePath);
        readonlyConfig.getOptional(HiveOptions.REMOTE_USER).ifPresent(hadoopConf::setRemoteUser);
        readonlyConfig.getOptional(HiveOptions.KRB5_PATH).ifPresent(hadoopConf::setKrb5Path);
        readonlyConfig
                .getOptional(HiveOptions.KERBEROS_PRINCIPAL)
                .ifPresent(hadoopConf::setKerberosPrincipal);
        readonlyConfig
                .getOptional(HiveOptions.KERBEROS_KEYTAB_PATH)
                .ifPresent(hadoopConf::setKerberosKeytabPath);
        return hadoopConf;
    }

    private Table getTableInformation() {
        if (tableInformation == null) {
            tableInformation = HiveTableUtils.getTableInfo(readonlyConfig);
        }
        return tableInformation;
    }

    private WriteStrategy getWriteStrategy() {
        if (writeStrategy == null) {
            writeStrategy = WriteStrategyFactory.of(fileSinkConfig.getFileFormat(), fileSinkConfig);
            writeStrategy.setCatalogTable(catalogTable);
        }
        return writeStrategy;
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}

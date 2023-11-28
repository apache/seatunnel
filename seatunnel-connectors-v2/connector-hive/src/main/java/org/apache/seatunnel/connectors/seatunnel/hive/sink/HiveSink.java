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
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;
import org.apache.seatunnel.connectors.seatunnel.file.sink.util.FileSystemUtils;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.WriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.WriteStrategyFactory;
import org.apache.seatunnel.connectors.seatunnel.hive.commit.HiveSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConstants;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveHadoopConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hive.sink.writter.HiveSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.hive.source.config.HiveSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveTableUtils;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import java.net.URI;
import java.net.URISyntaxException;
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
    private final Table tableInformation;
    private final CatalogTable catalogTable;
    private final ReadonlyConfig readonlyConfig;
    private final HiveHadoopConfig hiveHadoopConfig;
    private final FileSystemUtils fileSystemUtils;
    private final FileSinkConfig fileSinkConfig;
    private final WriteStrategy writeStrategy;
    private String jobId;

    public HiveSink(ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        this.readonlyConfig = readonlyConfig;
        this.catalogTable = catalogTable;
        this.tableInformation = HiveTableUtils.getTableInfo(readonlyConfig);
        this.hiveHadoopConfig = parseHiveHadoopConfig(readonlyConfig, tableInformation);
        this.fileSinkConfig =
                generateFileSinkConfig(readonlyConfig, tableInformation, catalogTable);
        this.writeStrategy =
                WriteStrategyFactory.of(fileSinkConfig.getFileFormat(), fileSinkConfig);
        this.fileSystemUtils = new FileSystemUtils(hiveHadoopConfig);
        this.writeStrategy.setSeaTunnelRowTypeInfo(catalogTable.getSeaTunnelRowType());
        this.writeStrategy.setFileSystemUtils(fileSystemUtils);
    }

    private FileSinkConfig generateFileSinkConfig(
            ReadonlyConfig readonlyConfig, Table tableInformation, CatalogTable catalogTable) {
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
                new HiveSinkAggregatedCommitter(readonlyConfig, tableInformation, fileSystemUtils));
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobId = jobContext.getJobId();
    }

    @Override
    public SinkWriter<SeaTunnelRow, FileCommitInfo, FileSinkState> restoreWriter(
            SinkWriter.Context context, List<FileSinkState> states) {
        return new HiveSinkWriter(writeStrategy, hiveHadoopConfig, context, jobId, states);
    }

    @Override
    public SinkWriter<SeaTunnelRow, FileCommitInfo, FileSinkState> createWriter(
            SinkWriter.Context context) {
        return new HiveSinkWriter(writeStrategy, hiveHadoopConfig, context, jobId);
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

    private HiveHadoopConfig parseHiveHadoopConfig(ReadonlyConfig readonlyConfig, Table table) {
        String hdfsLocation = tableInformation.getSd().getLocation();
        HiveHadoopConfig hiveHadoopConfig;
        try {
            URI uri = new URI(hdfsLocation);
            String path = uri.getPath();
            hiveHadoopConfig =
                    new HiveHadoopConfig(
                            hdfsLocation.replace(path, ""),
                            readonlyConfig.get(HiveSourceOptions.METASTORE_URI),
                            readonlyConfig.get(HiveSourceOptions.HIVE_SITE_PATH));
        } catch (URISyntaxException e) {
            String errorMsg =
                    String.format(
                            "Get hdfs namenode host from table location [%s] failed,"
                                    + "please check it",
                            hdfsLocation);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HDFS_NAMENODE_HOST_FAILED, errorMsg, e);
        }
        readonlyConfig
                .getOptional(HiveSourceOptions.HDFS_SITE_PATH)
                .ifPresent(hiveHadoopConfig::setHdfsSitePath);
        readonlyConfig
                .getOptional(HiveSourceOptions.KERBEROS_PRINCIPAL)
                .ifPresent(hiveHadoopConfig::setKerberosPrincipal);
        readonlyConfig
                .getOptional(HiveSourceOptions.KERBEROS_KEYTAB_PATH)
                .ifPresent(hiveHadoopConfig::setKerberosKeytabPath);
        return hiveHadoopConfig;
    }
}

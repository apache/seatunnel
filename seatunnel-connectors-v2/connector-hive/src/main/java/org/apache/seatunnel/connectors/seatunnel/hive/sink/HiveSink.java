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

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.sink.BaseHdfsFileSink;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hive.commit.HiveSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorException;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.auto.service.AutoService;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.FILE_FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.FILE_NAME_EXPRESSION;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.FILE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.IS_PARTITION_FIELD_WRITE_IN_FILE;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.PARTITION_BY;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.ROW_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.SINK_COLUMNS;
import static org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig.ORC_OUTPUT_FORMAT_CLASSNAME;
import static org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig.PARQUET_OUTPUT_FORMAT_CLASSNAME;
import static org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig.TEXT_OUTPUT_FORMAT_CLASSNAME;

@AutoService(SeaTunnelSink.class)
public class HiveSink extends BaseHdfsFileSink {
    private String dbName;
    private String tableName;
    private Table tableInformation;

    @Override
    public String getPluginName() {
        return "Hive";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig, HiveConfig.METASTORE_URI.key(), HiveConfig.TABLE_NAME.key());
        if (!result.isSuccess()) {
            throw new HiveConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
        if (pluginConfig.hasPath(BaseSinkConfig.PARTITION_DIR_EXPRESSION.key())) {
            throw new HiveConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "Hive sink connector does not support setting %s",
                            BaseSinkConfig.PARTITION_DIR_EXPRESSION.key()));
        }
        Pair<String[], Table> tableInfo = HiveConfig.getTableInfo(pluginConfig);
        dbName = tableInfo.getLeft()[0];
        tableName = tableInfo.getLeft()[1];
        tableInformation = tableInfo.getRight();
        List<String> sinkFields =
                tableInformation.getSd().getCols().stream()
                        .map(FieldSchema::getName)
                        .collect(Collectors.toList());
        List<String> partitionKeys =
                tableInformation.getPartitionKeys().stream()
                        .map(FieldSchema::getName)
                        .collect(Collectors.toList());
        sinkFields.addAll(partitionKeys);
        String outputFormat = tableInformation.getSd().getOutputFormat();
        if (TEXT_OUTPUT_FORMAT_CLASSNAME.equals(outputFormat)) {
            Map<String, String> parameters =
                    tableInformation.getSd().getSerdeInfo().getParameters();
            pluginConfig =
                    pluginConfig
                            .withValue(
                                    FILE_FORMAT.key(),
                                    ConfigValueFactory.fromAnyRef(FileFormat.TEXT.toString()))
                            .withValue(
                                    FIELD_DELIMITER.key(),
                                    ConfigValueFactory.fromAnyRef(parameters.get("field.delim")))
                            .withValue(
                                    ROW_DELIMITER.key(),
                                    ConfigValueFactory.fromAnyRef(parameters.get("line.delim")));
        } else if (PARQUET_OUTPUT_FORMAT_CLASSNAME.equals(outputFormat)) {
            pluginConfig =
                    pluginConfig.withValue(
                            FILE_FORMAT.key(),
                            ConfigValueFactory.fromAnyRef(FileFormat.PARQUET.toString()));
        } else if (ORC_OUTPUT_FORMAT_CLASSNAME.equals(outputFormat)) {
            pluginConfig =
                    pluginConfig.withValue(
                            FILE_FORMAT.key(),
                            ConfigValueFactory.fromAnyRef(FileFormat.ORC.toString()));
        } else {
            throw new HiveConnectorException(
                    CommonErrorCode.ILLEGAL_ARGUMENT,
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
        String hdfsLocation = tableInformation.getSd().getLocation();
        try {
            URI uri = new URI(hdfsLocation);
            String path = uri.getPath();
            hadoopConf = new HadoopConf(hdfsLocation.replace(path, ""));
            pluginConfig =
                    pluginConfig
                            .withValue(FILE_PATH.key(), ConfigValueFactory.fromAnyRef(path))
                            .withValue(
                                    FS_DEFAULT_NAME_KEY,
                                    ConfigValueFactory.fromAnyRef(hadoopConf.getHdfsNameKey()));
        } catch (URISyntaxException e) {
            String errorMsg =
                    String.format(
                            "Get hdfs namenode host from table location [%s] failed,"
                                    + "please check it",
                            hdfsLocation);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HDFS_NAMENODE_HOST_FAILED, errorMsg, e);
        }
        this.pluginConfig = pluginConfig;
        super.prepare(pluginConfig);
    }

    @Override
    public Optional<SinkAggregatedCommitter<FileCommitInfo, FileAggregatedCommitInfo>>
            createAggregatedCommitter() throws IOException {
        return Optional.of(
                new HiveSinkAggregatedCommitter(pluginConfig, dbName, tableName, fileSystemUtils));
    }
}

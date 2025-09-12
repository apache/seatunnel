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

package org.apache.seatunnel.connectors.seatunnel.file.hdfs.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.factory.BaseMultipleTableFileSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.config.HdfsSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;

import com.google.auto.service.AutoService;

import java.util.Arrays;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;

@AutoService(Factory.class)
public class HdfsFileSinkFactory extends BaseMultipleTableFileSinkFactory {
    @Override
    public String factoryIdentifier() {
        return FileSystemType.HDFS.getFileSystemPluginName();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HdfsSourceConfigOptions.DEFAULT_FS)
                .required(FileBaseSinkOptions.FILE_PATH)
                .optional(FileBaseSinkOptions.FILE_FORMAT_TYPE)
                .optional(SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .conditional(
                        FileBaseSinkOptions.FILE_FORMAT_TYPE,
                        FileFormat.TEXT,
                        FileBaseSinkOptions.ROW_DELIMITER,
                        FileBaseSinkOptions.FIELD_DELIMITER,
                        FileBaseSinkOptions.TXT_COMPRESS,
                        FileBaseSinkOptions.ENABLE_HEADER_WRITE)
                .conditional(
                        FileBaseSinkOptions.FILE_FORMAT_TYPE,
                        FileFormat.CSV,
                        FileBaseSinkOptions.ROW_DELIMITER,
                        FileBaseSinkOptions.TXT_COMPRESS,
                        FileBaseSinkOptions.ENABLE_HEADER_WRITE)
                .conditional(
                        FileBaseSinkOptions.FILE_FORMAT_TYPE,
                        FileFormat.JSON,
                        FileBaseSinkOptions.ROW_DELIMITER,
                        FileBaseSinkOptions.TXT_COMPRESS)
                .conditional(
                        FileBaseSinkOptions.FILE_FORMAT_TYPE,
                        FileFormat.ORC,
                        FileBaseSinkOptions.ORC_COMPRESS)
                .conditional(
                        FileBaseSinkOptions.FILE_FORMAT_TYPE,
                        FileFormat.PARQUET,
                        FileBaseSinkOptions.PARQUET_COMPRESS,
                        FileBaseSinkOptions.PARQUET_AVRO_WRITE_FIXED_AS_INT96,
                        FileBaseSinkOptions.PARQUET_AVRO_WRITE_TIMESTAMP_AS_INT96)
                .conditional(
                        FileBaseSinkOptions.FILE_FORMAT_TYPE,
                        FileFormat.XML,
                        FileBaseSinkOptions.XML_USE_ATTR_FORMAT,
                        FileBaseSinkOptions.XML_ROOT_TAG,
                        FileBaseSinkOptions.XML_ROW_TAG)
                .optional(FileBaseSinkOptions.CUSTOM_FILENAME)
                .conditional(
                        FileBaseSinkOptions.CUSTOM_FILENAME,
                        true,
                        FileBaseSinkOptions.FILE_NAME_EXPRESSION,
                        FileBaseSinkOptions.FILENAME_TIME_FORMAT)
                .optional(FileBaseSinkOptions.HAVE_PARTITION)
                .conditional(
                        FileBaseSinkOptions.HAVE_PARTITION,
                        true,
                        FileBaseSinkOptions.PARTITION_BY,
                        FileBaseSinkOptions.PARTITION_DIR_EXPRESSION,
                        FileBaseSinkOptions.IS_PARTITION_FIELD_WRITE_IN_FILE)
                .conditional(
                        FileBaseSinkOptions.FILE_FORMAT_TYPE,
                        Arrays.asList(
                                FileFormat.TEXT, FileFormat.JSON, FileFormat.CSV, FileFormat.XML),
                        FileBaseSinkOptions.ENCODING)
                .optional(FileBaseSinkOptions.SINK_COLUMNS)
                .optional(FileBaseSinkOptions.IS_ENABLE_TRANSACTION)
                .optional(FileBaseSinkOptions.DATE_FORMAT_LEGACY)
                .optional(FileBaseSinkOptions.DATETIME_FORMAT_LEGACY)
                .optional(FileBaseSinkOptions.TIME_FORMAT_LEGACY)
                .optional(FileBaseSinkOptions.SINGLE_FILE_MODE)
                .optional(FileBaseSinkOptions.BATCH_SIZE)
                .optional(FileBaseSinkOptions.HDFS_SITE_PATH)
                .optional(FileBaseSinkOptions.KERBEROS_PRINCIPAL)
                .optional(FileBaseSinkOptions.KERBEROS_KEYTAB_PATH)
                .optional(FileBaseSinkOptions.KRB5_PATH)
                .optional(FileBaseSinkOptions.REMOTE_USER)
                .optional(FileBaseSinkOptions.CREATE_EMPTY_FILE_WHEN_NO_DATA)
                .optional(FileBaseSinkOptions.FILENAME_EXTENSION)
                .optional(FileBaseSinkOptions.TMP_PATH)
                .build();
    }

    @Override
    public TableSink<SeaTunnelRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo>
            createSink(TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        HadoopConf hadoopConf = initHadoopConf(readonlyConfig);
        return () -> new HdfsFileSink(hadoopConf, readonlyConfig, catalogTable);
    }

    public HadoopConf initHadoopConf(ReadonlyConfig readonlyConfig) {
        Config pluginConfig = readonlyConfig.toConfig();
        CheckResult result =
                CheckConfigUtil.checkAllExists(readonlyConfig.toConfig(), FS_DEFAULT_NAME_KEY);
        if (!result.isSuccess()) {
            throw new FileConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            factoryIdentifier(), PluginType.SINK, result.getMsg()));
        }

        HadoopConf hadoopConf = new HadoopConf(pluginConfig.getString(FS_DEFAULT_NAME_KEY));

        if (pluginConfig.hasPath(FileBaseSinkOptions.HDFS_SITE_PATH.key())) {
            hadoopConf.setHdfsSitePath(
                    pluginConfig.getString(FileBaseSinkOptions.HDFS_SITE_PATH.key()));
        }

        if (pluginConfig.hasPath(FileBaseSinkOptions.REMOTE_USER.key())) {
            hadoopConf.setRemoteUser(pluginConfig.getString(FileBaseSinkOptions.REMOTE_USER.key()));
        }

        if (pluginConfig.hasPath(FileBaseSinkOptions.KRB5_PATH.key())) {
            hadoopConf.setKrb5Path(pluginConfig.getString(FileBaseSinkOptions.KRB5_PATH.key()));
        }

        if (pluginConfig.hasPath(FileBaseSinkOptions.KERBEROS_PRINCIPAL.key())) {
            hadoopConf.setKerberosPrincipal(
                    pluginConfig.getString(FileBaseSinkOptions.KERBEROS_PRINCIPAL.key()));
        }
        if (pluginConfig.hasPath(FileBaseSinkOptions.KERBEROS_KEYTAB_PATH.key())) {
            hadoopConf.setKerberosKeytabPath(
                    pluginConfig.getString(FileBaseSinkOptions.KERBEROS_KEYTAB_PATH.key()));
        }

        return hadoopConf;
    }
}

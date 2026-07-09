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
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.factory.BaseMultipleTableFileSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.config.HdfsFileSinkOptions;
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
                .required(HdfsFileSinkOptions.DEFAULT_FS)
                .required(HdfsFileSinkOptions.FILE_PATH)
                .optional(HdfsFileSinkOptions.FILE_FORMAT_TYPE)
                .optional(SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .conditional(
                        HdfsFileSinkOptions.FILE_FORMAT_TYPE,
                        FileFormat.TEXT,
                        HdfsFileSinkOptions.ROW_DELIMITER,
                        HdfsFileSinkOptions.FIELD_DELIMITER,
                        HdfsFileSinkOptions.TXT_COMPRESS,
                        HdfsFileSinkOptions.ENABLE_HEADER_WRITE)
                .conditional(
                        HdfsFileSinkOptions.FILE_FORMAT_TYPE,
                        FileFormat.CSV,
                        HdfsFileSinkOptions.ROW_DELIMITER,
                        HdfsFileSinkOptions.TXT_COMPRESS,
                        HdfsFileSinkOptions.ENABLE_HEADER_WRITE)
                .conditional(
                        HdfsFileSinkOptions.FILE_FORMAT_TYPE,
                        FileFormat.JSON,
                        HdfsFileSinkOptions.ROW_DELIMITER,
                        HdfsFileSinkOptions.TXT_COMPRESS)
                .conditional(
                        HdfsFileSinkOptions.FILE_FORMAT_TYPE,
                        FileFormat.ORC,
                        HdfsFileSinkOptions.ORC_COMPRESS)
                .conditional(
                        HdfsFileSinkOptions.FILE_FORMAT_TYPE,
                        FileFormat.PARQUET,
                        HdfsFileSinkOptions.PARQUET_COMPRESS,
                        HdfsFileSinkOptions.PARQUET_AVRO_WRITE_FIXED_AS_INT96,
                        HdfsFileSinkOptions.PARQUET_AVRO_WRITE_TIMESTAMP_AS_INT96)
                .conditional(
                        HdfsFileSinkOptions.FILE_FORMAT_TYPE,
                        FileFormat.XML,
                        HdfsFileSinkOptions.XML_USE_ATTR_FORMAT,
                        HdfsFileSinkOptions.XML_ROOT_TAG,
                        HdfsFileSinkOptions.XML_ROW_TAG)
                .optional(HdfsFileSinkOptions.CUSTOM_FILENAME)
                .conditional(
                        HdfsFileSinkOptions.CUSTOM_FILENAME,
                        true,
                        HdfsFileSinkOptions.FILE_NAME_EXPRESSION,
                        HdfsFileSinkOptions.FILENAME_TIME_FORMAT)
                .optional(HdfsFileSinkOptions.HAVE_PARTITION)
                .conditional(
                        HdfsFileSinkOptions.HAVE_PARTITION,
                        true,
                        HdfsFileSinkOptions.PARTITION_BY,
                        HdfsFileSinkOptions.PARTITION_DIR_EXPRESSION,
                        HdfsFileSinkOptions.IS_PARTITION_FIELD_WRITE_IN_FILE)
                .conditional(
                        HdfsFileSinkOptions.FILE_FORMAT_TYPE,
                        Arrays.asList(
                                FileFormat.TEXT, FileFormat.JSON, FileFormat.CSV, FileFormat.XML),
                        HdfsFileSinkOptions.ENCODING)
                .optional(HdfsFileSinkOptions.SINK_COLUMNS)
                .optional(HdfsFileSinkOptions.IS_ENABLE_TRANSACTION)
                .optional(HdfsFileSinkOptions.DATE_FORMAT_LEGACY)
                .optional(HdfsFileSinkOptions.DATETIME_FORMAT_LEGACY)
                .optional(HdfsFileSinkOptions.TIME_FORMAT_LEGACY)
                .optional(HdfsFileSinkOptions.SINGLE_FILE_MODE)
                .optional(HdfsFileSinkOptions.BATCH_SIZE)
                .optional(HdfsFileSinkOptions.HDFS_SITE_PATH)
                .optional(HdfsFileSinkOptions.KERBEROS_PRINCIPAL)
                .optional(HdfsFileSinkOptions.KERBEROS_KEYTAB_PATH)
                .optional(HdfsFileSinkOptions.KRB5_PATH)
                .optional(HdfsFileSinkOptions.REMOTE_USER)
                .optional(HdfsFileSinkOptions.CREATE_EMPTY_FILE_WHEN_NO_DATA)
                .optional(HdfsFileSinkOptions.FILENAME_EXTENSION)
                .optional(HdfsFileSinkOptions.TMP_PATH)
                .optional(HdfsFileSinkOptions.SCHEMA_SAVE_MODE)
                .optional(HdfsFileSinkOptions.DATA_SAVE_MODE)
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

        if (pluginConfig.hasPath(HdfsFileSinkOptions.HDFS_SITE_PATH.key())) {
            hadoopConf.setHdfsSitePath(
                    pluginConfig.getString(HdfsFileSinkOptions.HDFS_SITE_PATH.key()));
        }

        if (pluginConfig.hasPath(HdfsFileSinkOptions.REMOTE_USER.key())) {
            hadoopConf.setRemoteUser(pluginConfig.getString(HdfsFileSinkOptions.REMOTE_USER.key()));
        }

        if (pluginConfig.hasPath(HdfsFileSinkOptions.KRB5_PATH.key())) {
            hadoopConf.setKrb5Path(pluginConfig.getString(HdfsFileSinkOptions.KRB5_PATH.key()));
        }

        if (pluginConfig.hasPath(HdfsFileSinkOptions.KERBEROS_PRINCIPAL.key())) {
            hadoopConf.setKerberosPrincipal(
                    pluginConfig.getString(HdfsFileSinkOptions.KERBEROS_PRINCIPAL.key()));
        }
        if (pluginConfig.hasPath(HdfsFileSinkOptions.KERBEROS_KEYTAB_PATH.key())) {
            hadoopConf.setKerberosKeytabPath(
                    pluginConfig.getString(HdfsFileSinkOptions.KERBEROS_KEYTAB_PATH.key()));
        }

        return hadoopConf;
    }
}

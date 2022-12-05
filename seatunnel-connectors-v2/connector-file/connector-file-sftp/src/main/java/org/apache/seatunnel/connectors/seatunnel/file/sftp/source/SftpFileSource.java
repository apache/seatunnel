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

package org.apache.seatunnel.connectors.seatunnel.file.sftp.source;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.sftp.config.SftpConf;
import org.apache.seatunnel.connectors.seatunnel.file.sftp.config.SftpConfig;
import org.apache.seatunnel.connectors.seatunnel.file.source.BaseFileSource;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategyFactory;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

import java.io.IOException;

@AutoService(SeaTunnelSource.class)
public class SftpFileSource extends BaseFileSource {
    @Override
    public String getPluginName() {
        return FileSystemType.SFTP.getFileSystemPluginName();
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig,
                SftpConfig.FILE_PATH.key(), SftpConfig.FILE_TYPE.key(),
                SftpConfig.SFTP_HOST.key(), SftpConfig.SFTP_PORT.key(),
                SftpConfig.SFTP_USERNAME.key(), SftpConfig.SFTP_PASSWORD.key());
        if (!result.isSuccess()) {
            throw new FileConnectorException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format("PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        FileFormat fileFormat = FileFormat.valueOf(pluginConfig.getString(SftpConfig.FILE_TYPE.key()).toUpperCase());
        if (fileFormat == FileFormat.ORC || fileFormat == FileFormat.PARQUET) {
            throw new FileConnectorException(CommonErrorCode.ILLEGAL_ARGUMENT,
                    "Sftp file source connector only support read [text, csv, json] files");
        }
        readStrategy = ReadStrategyFactory.of(pluginConfig.getString(SftpConfig.FILE_TYPE.key()));
        readStrategy.setPluginConfig(pluginConfig);
        String path = pluginConfig.getString(SftpConfig.FILE_PATH.key());
        hadoopConf = SftpConf.buildWithConfig(pluginConfig);
        try {
            filePaths = readStrategy.getFileNamesByPath(hadoopConf, path);
        } catch (IOException e) {
            String errorMsg = String.format("Get file list from this path [%s] failed", path);
            throw new FileConnectorException(FileConnectorErrorCode.FILE_LIST_GET_FAILED, errorMsg, e);
        }
        // support user-defined schema
        // only json csv text type support user-defined schema now
        if (pluginConfig.hasPath(SeaTunnelSchema.SCHEMA.key())) {
            switch (fileFormat) {
                case CSV:
                case TEXT:
                case JSON:
                    Config schemaConfig = pluginConfig.getConfig(SeaTunnelSchema.SCHEMA.key());
                    SeaTunnelRowType userDefinedSchema = SeaTunnelSchema
                            .buildWithConfig(schemaConfig)
                            .getSeaTunnelRowType();
                    readStrategy.setSeaTunnelRowTypeInfo(userDefinedSchema);
                    rowType = readStrategy.getActualSeaTunnelRowTypeInfo();
                    break;
                case ORC:
                case PARQUET:
                    throw new FileConnectorException(CommonErrorCode.UNSUPPORTED_OPERATION,
                            "SeaTunnel does not support user-defined schema for [parquet, orc] files");
                default:
                    // never got in there
                    throw new FileConnectorException(CommonErrorCode.ILLEGAL_ARGUMENT,
                            "SeaTunnel does not supported this file format");
            }
        } else {
            try {
                rowType = readStrategy.getSeaTunnelRowTypeInfo(hadoopConf, filePaths.get(0));
            } catch (FileConnectorException e) {
                String errorMsg = String.format("Get table schema from file [%s] failed", filePaths.get(0));
                throw new FileConnectorException(CommonErrorCode.TABLE_SCHEMA_GET_FAILED, errorMsg, e);
            }
        }
    }
}

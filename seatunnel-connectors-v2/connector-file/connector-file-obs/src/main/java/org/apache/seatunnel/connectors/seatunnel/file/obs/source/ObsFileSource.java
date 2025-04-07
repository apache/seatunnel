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

package org.apache.seatunnel.connectors.seatunnel.file.obs.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.obs.config.ObsConf;
import org.apache.seatunnel.connectors.seatunnel.file.obs.config.ObsConfig;
import org.apache.seatunnel.connectors.seatunnel.file.source.BaseFileSource;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategyFactory;

import com.google.auto.service.AutoService;

import java.io.IOException;

@AutoService(SeaTunnelSource.class)
public class ObsFileSource extends BaseFileSource {
    @Override
    public String getPluginName() {
        return FileSystemType.OBS.getFileSystemPluginName();
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        ObsConfig.FILE_PATH.key(),
                        ObsConfig.FILE_FORMAT_TYPE.key(),
                        ObsConfig.ENDPOINT.key(),
                        ObsConfig.ACCESS_KEY.key(),
                        ObsConfig.ACCESS_SECRET.key(),
                        ObsConfig.BUCKET.key());
        if (!result.isSuccess()) {
            throw new FileConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        readStrategy =
                ReadStrategyFactory.of(pluginConfig.getString(ObsConfig.FILE_FORMAT_TYPE.key()));
        readStrategy.setPluginConfig(pluginConfig);
        hadoopConf = ObsConf.buildWithConfig(pluginConfig);
        readStrategy.init(hadoopConf);
        String path = pluginConfig.getString(ObsConfig.FILE_PATH.key());
        try {
            filePaths = readStrategy.getFileNamesByPath(path);
        } catch (IOException e) {
            String errorMsg = String.format("Get file list from this path [%s] failed", path);
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_LIST_GET_FAILED, errorMsg, e);
        }
        // support user-defined schema
        FileFormat fileFormat =
                FileFormat.valueOf(
                        pluginConfig.getString(ObsConfig.FILE_FORMAT_TYPE.key()).toUpperCase());
        // only json text csv type support user-defined schema now
        if (pluginConfig.hasPath(ConnectorCommonOptions.SCHEMA.key())) {
            switch (fileFormat) {
                case CSV:
                case TEXT:
                case JSON:
                case EXCEL:
                    CatalogTable userDefinedCatalogTable =
                            CatalogTableUtil.buildWithConfig(pluginConfig);
                    readStrategy.setCatalogTable(userDefinedCatalogTable);
                    rowType = readStrategy.getActualSeaTunnelRowTypeInfo();
                    break;
                case ORC:
                case PARQUET:
                    throw new FileConnectorException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                            "SeaTunnel does not support user-defined schema for [parquet, orc] files");
                default:
                    // never got in there
                    throw new FileConnectorException(
                            CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                            "SeaTunnel does not supported this file format");
            }
        } else {
            if (filePaths.isEmpty()) {
                // When the directory is empty, distribute default behavior schema
                rowType = CatalogTableUtil.buildSimpleTextSchema();
                return;
            }
            try {
                rowType = readStrategy.getSeaTunnelRowTypeInfo(filePaths.get(0));
            } catch (FileConnectorException e) {
                String errorMsg =
                        String.format("Get table schema from file [%s] failed", filePaths.get(0));
                throw new FileConnectorException(
                        CommonErrorCodeDeprecated.TABLE_SCHEMA_GET_FAILED, errorMsg, e);
            }
        }
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.file.oss.source;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FilePluginException;
import org.apache.seatunnel.connectors.seatunnel.file.oss.source.config.OssConf;
import org.apache.seatunnel.connectors.seatunnel.file.oss.source.config.OssSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.source.BaseFileSource;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategyFactory;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import org.apache.hadoop.fs.aliyun.oss.Constants;

import java.io.IOException;
import java.util.HashMap;

@AutoService(SeaTunnelSource.class)
public class OssFileSource extends BaseFileSource {
    @Override
    public String getPluginName() {
        return FileSystemType.OSS.getFileSystemPluginName();
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig,
                OssSourceConfig.FILE_PATH, OssSourceConfig.FILE_TYPE,
                OssSourceConfig.BUCKET, OssSourceConfig.ACCESS_KEY,
                OssSourceConfig.ACCESS_SECRET, OssSourceConfig.BUCKET);
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, result.getMsg());
        }
        readStrategy = ReadStrategyFactory.of(pluginConfig.getString(OssSourceConfig.FILE_TYPE));
        String path = pluginConfig.getString(OssSourceConfig.FILE_PATH);
        hadoopConf = new OssConf(pluginConfig.getString(OssSourceConfig.BUCKET));
        HashMap<String, String> ossOptions = new HashMap<>();
        ossOptions.put(Constants.ACCESS_KEY_ID, pluginConfig.getString(OssSourceConfig.ACCESS_KEY));
        ossOptions.put(Constants.ACCESS_KEY_SECRET, pluginConfig.getString(OssSourceConfig.ACCESS_SECRET));
        ossOptions.put(Constants.ENDPOINT_KEY, pluginConfig.getString(OssSourceConfig.ENDPOINT));
        hadoopConf.setExtraOptions(ossOptions);
        try {
            filePaths = readStrategy.getFileNamesByPath(hadoopConf, path);
        } catch (IOException e) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, "Check file path fail.");
        }
        // support user-defined schema
        if (pluginConfig.hasPath(OssSourceConfig.SCHEMA)) {
            Config schemaConfig = pluginConfig.getConfig(OssSourceConfig.SCHEMA);
            rowType = SeaTunnelSchema
                    .buildWithConfig(schemaConfig)
                    .getSeaTunnelRowType();
            readStrategy.setSeaTunnelRowTypeInfo(rowType);
        } else {
            try {
                rowType = readStrategy.getSeaTunnelRowTypeInfo(hadoopConf, filePaths.get(0));
            } catch (FilePluginException e) {
                throw new PrepareFailException(getPluginName(), PluginType.SOURCE, "Read file schema error.", e);
            }
        }
    }
}

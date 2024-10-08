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

package org.apache.seatunnel.connectors.seatunnel.file.oss.jindo.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.oss.jindo.config.OssConf;
import org.apache.seatunnel.connectors.seatunnel.file.oss.jindo.config.OssConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.oss.jindo.exception.OssJindoConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.sink.BaseFileSink;

import com.google.auto.service.AutoService;

import java.util.Optional;

@AutoService(SeaTunnelSink.class)
public class OssFileSink extends BaseFileSink {
    @Override
    public String getPluginName() {
        return FileSystemType.OSS_JINDO.getFileSystemPluginName();
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        super.prepare(pluginConfig);
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        OssConfigOptions.FILE_PATH.key(),
                        OssConfigOptions.ENDPOINT.key(),
                        OssConfigOptions.ACCESS_KEY.key(),
                        OssConfigOptions.ACCESS_SECRET.key(),
                        OssConfigOptions.BUCKET.key());
        if (!result.isSuccess()) {
            throw new OssJindoConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
        hadoopConf = OssConf.buildWithConfig(pluginConfig);
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return super.getWriteCatalogTable();
    }
}

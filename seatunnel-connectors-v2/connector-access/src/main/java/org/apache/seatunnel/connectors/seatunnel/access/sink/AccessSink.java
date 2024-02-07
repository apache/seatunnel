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

package org.apache.seatunnel.connectors.seatunnel.access.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.access.client.AccessClient;
import org.apache.seatunnel.connectors.seatunnel.access.config.AccessParameters;
import org.apache.seatunnel.connectors.seatunnel.access.exception.AccessConnectorException;
import org.apache.seatunnel.connectors.seatunnel.access.util.TypeConvertUtil;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import com.google.auto.service.AutoService;

import java.io.IOException;

import static org.apache.seatunnel.connectors.seatunnel.access.config.AccessConfig.DRIVER;
import static org.apache.seatunnel.connectors.seatunnel.access.config.AccessConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.access.config.AccessConfig.QUERY;
import static org.apache.seatunnel.connectors.seatunnel.access.config.AccessConfig.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.access.config.AccessConfig.URL;
import static org.apache.seatunnel.connectors.seatunnel.access.config.AccessConfig.USERNAME;

@AutoService(SeaTunnelSink.class)
public class AccessSink extends AbstractSimpleSink<SeaTunnelRow, Void> {
    private SeaTunnelRowType seaTunnelRowType;
    private final AccessParameters accessParameters = new AccessParameters();

    private SeaTunnelDataType<?>[] seaTunnelDataTypes;

    @Override
    public String getPluginName() {
        return "Access";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult checkResult =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        DRIVER.key(),
                        URL.key(),
                        USERNAME.key(),
                        PASSWORD.key(),
                        TABLE.key(),
                        QUERY.key());
        if (!checkResult.isSuccess()) {
            throw new AccessConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, checkResult.getMsg()));
        }
        this.accessParameters.buildWithConfig(pluginConfig);

        AccessClient accessClient =
                new AccessClient(
                        pluginConfig.getString(DRIVER.key()),
                        pluginConfig.getString(URL.key()),
                        pluginConfig.getString(USERNAME.key()),
                        pluginConfig.getString(PASSWORD.key()),
                        pluginConfig.getString(QUERY.key()));

        seaTunnelDataTypes =
                TypeConvertUtil.getSeaTunnelDataTypes(
                        accessParameters, accessClient, pluginConfig, getPluginName());
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.seaTunnelRowType;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context)
            throws IOException {
        return new AccessSinkWriter(accessParameters, seaTunnelDataTypes);
    }
}

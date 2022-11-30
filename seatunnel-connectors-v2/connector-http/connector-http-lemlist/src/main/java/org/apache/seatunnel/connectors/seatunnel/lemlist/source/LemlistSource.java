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

package org.apache.seatunnel.connectors.seatunnel.lemlist.source;

import static org.apache.seatunnel.connectors.seatunnel.http.util.AuthorizationUtil.getTokenByBasicAuth;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSource;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceReader;
import org.apache.seatunnel.connectors.seatunnel.lemlist.source.config.LemlistSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.lemlist.source.config.LemlistSourceParameter;
import org.apache.seatunnel.connectors.seatunnel.lemlist.source.exception.LemlistConnectorException;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AutoService(SeaTunnelSource.class)
public class LemlistSource extends HttpSource {
    private final LemlistSourceParameter lemlistSourceParameter = new LemlistSourceParameter();
    @Override
    public String getPluginName() {
        return "Lemlist";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig, LemlistSourceConfig.URL.key(), LemlistSourceConfig.PASSWORD.key());
        if (!result.isSuccess()) {
            throw new LemlistConnectorException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format("PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        //get accessToken by basic auth
        String accessToken = getTokenByBasicAuth("", pluginConfig.getString(LemlistSourceConfig.PASSWORD.key()));
        lemlistSourceParameter.buildWithConfig(pluginConfig, accessToken);
        buildSchemaWithConfig(pluginConfig);
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext readerContext) throws Exception {
        return new HttpSourceReader(this.lemlistSourceParameter, readerContext, this.deserializationSchema, jsonField, contentField);
    }
}

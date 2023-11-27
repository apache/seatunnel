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

package org.apache.seatunnel.connectors.seatunnel.klaviyo.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSource;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceReader;
import org.apache.seatunnel.connectors.seatunnel.klaviyo.source.config.KlaviyoSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.klaviyo.source.config.KlaviyoSourceParameter;
import org.apache.seatunnel.connectors.seatunnel.klaviyo.source.config.exception.KlaviyoConnectorException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KlaviyoSource extends HttpSource {
    private final KlaviyoSourceParameter klaviyoSourceParameter = new KlaviyoSourceParameter();

    public KlaviyoSource(Config pluginConfig) {
        super(pluginConfig);
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        KlaviyoSourceConfig.URL.key(),
                        KlaviyoSourceConfig.PRIVATE_KEY.key(),
                        KlaviyoSourceConfig.REVISION.key());
        if (!result.isSuccess()) {
            throw new KlaviyoConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        this.klaviyoSourceParameter.buildWithConfig(pluginConfig);
    }

    @Override
    public String getPluginName() {
        return "Klaviyo";
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new HttpSourceReader(
                this.klaviyoSourceParameter,
                readerContext,
                this.deserializationSchema,
                jsonField,
                contentField);
    }
}

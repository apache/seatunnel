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

package org.apache.seatunnel.connectors.seatunnel.amazonsqs.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.exception.AmazonSqsConnectorException;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsConfig.ACCESS_KEY_ID;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsConfig.QUEUE;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsConfig.REGION;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsConfig.SECRET_ACCESS_KEY;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsConfig.URL;

@Slf4j
@AutoService(SeaTunnelSource.class)
public class AmazonSqsSource extends AbstractSingleSplitSource<SeaTunnelRow>
        implements SupportColumnProjection {

    private AmazonSqsSourceOptions AmazonSqsSourceOptions;

    private SeaTunnelRowType typeInfo;

    @Override
    public String getPluginName() {
        return "AmazonDynamodb";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        URL.key(),
                        QUEUE.key(),
                        REGION.key(),
                        ACCESS_KEY_ID.key(),
                        SECRET_ACCESS_KEY.key());
        if (!result.isSuccess()) {
            throw new AmazonSqsConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        AmazonSqsSourceOptions = new AmazonSqsSourceOptions(pluginConfig);
        typeInfo = CatalogTableUtil.buildWithConfig(pluginConfig).getSeaTunnelRowType();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.typeInfo;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new AmazonSqsSourceReader(readerContext, AmazonSqsSourceOptions, typeInfo);
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.gcp.pubsub.source;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.gcp.pubsub.config.PubSubConfig;
import org.apache.seatunnel.connectors.seatunnel.gcp.pubsub.config.PubSubParameters;
import org.apache.seatunnel.connectors.seatunnel.gcp.pubsub.exception.PubSubConnectorException;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

@AutoService(SeaTunnelSource.class)
public class PubSubSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    private SeaTunnelRowType rowType;

    private PubSubParameters parameters;

    private DeserializationSchema<SeaTunnelRow> deserializationSchema;

    private JobContext jobContext;

    @Override
    public String getPluginName() {
        return "GcpPubSub";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult checkResult = CheckConfigUtil.checkAllExists(
                pluginConfig,
                PubSubConfig.TOPIC.key(),
                PubSubConfig.SUBSCRIPTION_ID.key(),
                PubSubConfig.SERVICE_ACCOUNT_KEYS.key()
        );

        if (!checkResult.isSuccess()) {
            throw new PubSubConnectorException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, checkResult.getMsg())
            );
        }

        this.parameters = PubSubParameters.of(pluginConfig);
        this.rowType = pluginConfig.hasPath(CatalogTableUtil.SCHEMA.key()) ?
                CatalogTableUtil.buildWithConfig(pluginConfig).getSeaTunnelRowType() :
                CatalogTableUtil.buildSimpleTextSchema();

        this.deserializationSchema  = new JsonDeserializationSchema(false, false, rowType);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return rowType;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext readerContext) throws Exception {
        return new PubSubSourceReader(parameters, rowType, deserializationSchema);
    }
}

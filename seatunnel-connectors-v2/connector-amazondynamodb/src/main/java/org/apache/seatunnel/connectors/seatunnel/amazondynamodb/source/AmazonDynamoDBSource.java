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

package org.apache.seatunnel.connectors.seatunnel.amazondynamodb.source;

import static org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBConfig.ACCESS_KEY_ID;
import static org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBConfig.REGION;
import static org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBConfig.SECRET_ACCESS_KEY;
import static org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBConfig.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBConfig.URL;
import static org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema.SCHEMA;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.exception.AmazonDynamoDBConnectorException;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AutoService(SeaTunnelSource.class)
public class AmazonDynamoDBSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    private AmazonDynamoDBSourceOptions amazondynamodbSourceOptions;

    private SeaTunnelRowType typeInfo;

    @Override
    public String getPluginName() {
        return "AmazonDynamodb";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig, URL.key(), TABLE.key(), REGION.key(), ACCESS_KEY_ID.key(), SECRET_ACCESS_KEY.key(), SCHEMA.key());
        if (!result.isSuccess()) {
            throw new AmazonDynamoDBConnectorException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format("PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        amazondynamodbSourceOptions = new AmazonDynamoDBSourceOptions(pluginConfig);
        typeInfo = SeaTunnelSchema.buildWithConfig(amazondynamodbSourceOptions.getSchema()).getSeaTunnelRowType();
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
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext readerContext) throws Exception {
        return new AmazonDynamoDBSourceReader(readerContext, amazondynamodbSourceOptions, typeInfo);
    }
}

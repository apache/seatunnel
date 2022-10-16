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

package org.apache.seatunnel.connectors.seatunnel.redis.source;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisConfig;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

@AutoService(SeaTunnelSource.class)
public class RedisSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    private final RedisParameters redisParameters = new RedisParameters();
    private SeaTunnelRowType seaTunnelRowType;
    private DeserializationSchema<SeaTunnelRow> deserializationSchema;

    @Override
    public String getPluginName() {
        return "Redis";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig, RedisConfig.HOST, RedisConfig.PORT, RedisConfig.KEY_PATTERN, RedisConfig.DATA_TYPE);
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, result.getMsg());
        }
        this.redisParameters.buildWithConfig(pluginConfig);
        if (pluginConfig.hasPath(SeaTunnelSchema.SCHEMA)) {
            Config schema = pluginConfig.getConfig(SeaTunnelSchema.SCHEMA);
            this.seaTunnelRowType = SeaTunnelSchema.buildWithConfig(schema).getSeaTunnelRowType();
        } else {
            this.seaTunnelRowType = SeaTunnelSchema.buildSimpleTextSchema();
        }
        // TODO: use format SPI
        // default use json format
        String format;
        if (pluginConfig.hasPath(RedisConfig.FORMAT)) {
            format = pluginConfig.getString(RedisConfig.FORMAT);
            this.deserializationSchema = null;
        } else {
            format = "json";
            this.deserializationSchema = new JsonDeserializationSchema(false, false, seaTunnelRowType);
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return seaTunnelRowType;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext readerContext) throws Exception {
        return new RedisSourceReader(redisParameters, readerContext, deserializationSchema);
    }
}

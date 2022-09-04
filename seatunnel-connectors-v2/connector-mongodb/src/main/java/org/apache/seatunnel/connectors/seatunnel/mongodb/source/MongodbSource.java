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

package org.apache.seatunnel.connectors.seatunnel.mongodb.source;

import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.COLLECTION;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.DEFAULT_FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.SCHEMA;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.URI;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbParameters;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigBeanFactory;

import com.google.auto.service.AutoService;

@AutoService(SeaTunnelSource.class)
public class MongodbSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    private SeaTunnelContext seaTunnelContext;

    private SeaTunnelRowType rowType;

    private MongodbParameters params;

    private DeserializationSchema<SeaTunnelRow> deserializationSchema;

    private static final String CONF_PREFIX = "readconfig.";

    @Override
    public String getPluginName() {
        return "MongoDB";
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        Config extractConfig = TypesafeConfigUtils.extractSubConfig(config, CONF_PREFIX, false);

        CheckResult result = CheckConfigUtil.checkAllExists(extractConfig, URI, DATABASE, COLLECTION);
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, result.getMsg());
        }

        this.params = ConfigBeanFactory.create(extractConfig, MongodbParameters.class);

        if (extractConfig.hasPath(SCHEMA)) {
            Config schema = extractConfig.getConfig(SCHEMA);
            this.rowType = SeaTunnelSchema.buildWithConfig(schema).getSeaTunnelRowType();
        } else {
            this.rowType = SeaTunnelSchema.buildSimpleTextSchema();
        }

        // TODO: use format SPI
        // default use json format
        String format;
        if (extractConfig.hasPath(FORMAT)) {
            format = extractConfig.getString(FORMAT);
            this.deserializationSchema = null;
        } else {
            format = DEFAULT_FORMAT;
            this.deserializationSchema = new JsonDeserializationSchema(false, false, rowType);
        }
    }

    @Override
    public void setSeaTunnelContext(SeaTunnelContext seaTunnelContext) {
        this.seaTunnelContext = seaTunnelContext;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.rowType;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext context) throws Exception {
        return new MongodbSourceReader(context, this.params, this.deserializationSchema);
    }

}

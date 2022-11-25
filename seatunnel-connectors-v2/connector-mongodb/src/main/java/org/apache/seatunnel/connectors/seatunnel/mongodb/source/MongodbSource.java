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
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.URI;

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
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbParameters;
import org.apache.seatunnel.connectors.seatunnel.mongodb.exception.MongodbConnectorException;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigBeanFactory;

import com.google.auto.service.AutoService;

@AutoService(SeaTunnelSource.class)
public class MongodbSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    private SeaTunnelRowType rowType;

    private MongodbParameters params;

    @Override
    public String getPluginName() {
        return "MongoDB";
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(config, URI.key(), DATABASE.key(), COLLECTION.key());
        if (!result.isSuccess()) {
            throw new MongodbConnectorException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                String.format("PluginName: %s, PluginType: %s, Message: %s",
                    getPluginName(), PluginType.SOURCE, result.getMsg()));
        }

        this.params = ConfigBeanFactory.create(config, MongodbParameters.class);

        if (config.hasPath(SeaTunnelSchema.SCHEMA.key())) {
            Config schema = config.getConfig(SeaTunnelSchema.SCHEMA.key());
            this.rowType = SeaTunnelSchema.buildWithConfig(schema).getSeaTunnelRowType();
        } else {
            this.rowType = SeaTunnelSchema.buildSimpleTextSchema();
        }
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
        boolean useSimpleTextSchema = SeaTunnelSchema.buildSimpleTextSchema().equals(rowType);
        return new MongodbSourceReader(context, this.params, rowType, useSimpleTextSchema);
    }
}

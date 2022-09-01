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

import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.Config.COLLECTION;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.Config.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.Config.SCHEMA;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.Config.URI;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.mongodb.state.MongodbSourceState;
import org.apache.seatunnel.connectors.seatunnel.mongodb.utils.RawTypeConverter;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigBeanFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auto.service.AutoService;

import java.util.Iterator;
import java.util.Map;

@AutoService(SeaTunnelSource.class)
public class MongodbSource implements SeaTunnelSource<SeaTunnelRow, MongodbSourceSplit, MongodbSourceState> {

    private SeaTunnelRowType rowTypeInfo;

    private MongodbSourceEvent params;

    String confPrefix = "readconfig.";

    @Override
    public String getPluginName() {
        return "MongoDB";
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        Config extractConfig = TypesafeConfigUtils.extractSubConfig(config, confPrefix, false);
        this.params = ConfigBeanFactory.create(extractConfig, MongodbSourceEvent.class);

        CheckResult result = CheckConfigUtil.checkAllExists(extractConfig, URI, DATABASE, COLLECTION);
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, result.getMsg());
        }

        if (extractConfig.hasPath(SCHEMA)) {
            ObjectNode schemaJson = JsonUtils.parseObject(extractConfig.getString(SCHEMA));
            Iterator<Map.Entry<String, JsonNode>> fields = schemaJson.fields();

            String[] fieldNames = new String[schemaJson.size()];
            SeaTunnelDataType<?>[] dataTypes = new SeaTunnelDataType[schemaJson.size()];

            int i = 0;
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String key = entry.getKey();
                String value = entry.getValue().asText();
                fieldNames[i] = key;
                dataTypes[i] = RawTypeConverter.convert(value);
                i++;
            }

            this.rowTypeInfo = new SeaTunnelRowType(fieldNames, dataTypes);
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.rowTypeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, MongodbSourceSplit> createReader(SourceReader.Context context) throws Exception {
        return new MongodbSourceReader(context, this.params, rowTypeInfo);
    }

    @Override
    public SourceSplitEnumerator<MongodbSourceSplit, MongodbSourceState> createEnumerator(SourceSplitEnumerator.Context<MongodbSourceSplit> enumeratorContext) throws Exception {
        return new MongodbSourceSplitEnumerator(enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<MongodbSourceSplit, MongodbSourceState> restoreEnumerator(SourceSplitEnumerator.Context<MongodbSourceSplit> enumeratorContext, MongodbSourceState checkpointState) throws Exception {
        return new MongodbSourceSplitEnumerator(enumeratorContext);
    }
}

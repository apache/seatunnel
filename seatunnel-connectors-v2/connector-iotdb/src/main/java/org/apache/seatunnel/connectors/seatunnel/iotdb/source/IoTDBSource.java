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

package org.apache.seatunnel.connectors.seatunnel.iotdb.source;

import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.HOST;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.NODE_URLS;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.PORT;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.constant.SourceConstants.FIELDS_K_V_SPLIT;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.constant.SourceConstants.FIELDS_SPLIT;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.iotdb.state.IoTDBSourceState;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.Map;

@AutoService(SeaTunnelSource.class)
public class IoTDBSource implements SeaTunnelSource<SeaTunnelRow, IoTDBSourceSplit, IoTDBSourceState> {

    private SeaTunnelContext seaTunnelContext;

    private SeaTunnelRowType typeInfo;

    private Map<String, Object> configParams = new HashMap();

    @Override
    public String getPluginName() {
        return "IoTDB";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig, HOST, PORT);
        if (!result.isSuccess()) {
            result = CheckConfigUtil.checkAllExists(pluginConfig, NODE_URLS);

            if (!result.isSuccess()) {
                throw new PrepareFailException(getPluginName(), PluginType.SOURCE, "host and port and node urls are both empty");
            }
        }
        if (pluginConfig.getString(FIELDS) == null) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, "fields is empty");
        }
        this.typeInfo = convertToType(pluginConfig.getString(FIELDS));

        pluginConfig.entrySet().forEach(entry -> configParams.put(entry.getKey(), entry.getValue().unwrapped()));
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
    public SourceReader<SeaTunnelRow, IoTDBSourceSplit> createReader(SourceReader.Context readerContext) {
        return new IoTDBSourceReader(configParams, readerContext, typeInfo);
    }

    @Override
    public SourceSplitEnumerator<IoTDBSourceSplit, IoTDBSourceState> createEnumerator(SourceSplitEnumerator.Context<IoTDBSourceSplit> enumeratorContext) throws Exception {
        return new IoTDBSourceSplitEnumerator(enumeratorContext, configParams);
    }

    @Override
    public SourceSplitEnumerator<IoTDBSourceSplit, IoTDBSourceState> restoreEnumerator(SourceSplitEnumerator.Context<IoTDBSourceSplit> enumeratorContext, IoTDBSourceState checkpointState) throws Exception {
        return new IoTDBSourceSplitEnumerator(enumeratorContext, checkpointState, configParams);
    }

    private SeaTunnelRowType convertToType(String fields) {
        String[] fieldArray = fields.split(FIELDS_SPLIT);

        String[] seatunnelFields = new String[fieldArray.length];
        SeaTunnelDataType[] stunnelTypes = new SeaTunnelDataType[fieldArray.length];

        for (int i = 0; i < fieldArray.length; i++) {
            String[] kv = fieldArray[i].split(FIELDS_K_V_SPLIT);
            seatunnelFields[i] = kv[0];
            stunnelTypes[i] = convertToDataType(kv[1]);
        }
        return new SeaTunnelRowType(seatunnelFields, stunnelTypes);
    }

    private SeaTunnelDataType convertToDataType(String type) {

        switch (type) {
            case "INT32":
                return BasicType.INT_TYPE;
            case "INT64":
                return BasicType.LONG_TYPE;
            case "FLOAT":
                return BasicType.FLOAT_TYPE;
            case "DOUBLE":
                return BasicType.DOUBLE_TYPE;
            case "TEXT":
                return BasicType.STRING_TYPE;
            case "BOOLEAN":
                return BasicType.BOOLEAN_TYPE;
            default:
                throw new IllegalArgumentException("unknown type: " + type);
        }
    }
}


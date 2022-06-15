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

package org.apache.seatunnel.connectors.seatunnel.http.source;

import static org.apache.seatunnel.connectors.seatunnel.http.config.Config.BODY;
import static org.apache.seatunnel.connectors.seatunnel.http.config.Config.HEADERS;
import static org.apache.seatunnel.connectors.seatunnel.http.config.Config.METHOD;
import static org.apache.seatunnel.connectors.seatunnel.http.config.Config.METHOD_DEFAULT_VALUE;
import static org.apache.seatunnel.connectors.seatunnel.http.config.Config.PARAMS;
import static org.apache.seatunnel.connectors.seatunnel.http.config.Config.URL;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
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
import org.apache.seatunnel.connectors.seatunnel.http.state.HttpState;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

import java.util.Map;
import java.util.stream.Collectors;

@AutoService(SeaTunnelSource.class)
public class HttpSource implements SeaTunnelSource<SeaTunnelRow, HttpSourceSplit, HttpState> {
    private final HttpSourceParameter parameter = new HttpSourceParameter();
    private SeaTunnelRowType rowType;
    private SeaTunnelContext seaTunnelContext;
    @Override
    public String getPluginName() {
        return "Http";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig, URL);
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, result.getMsg());
        }
        this.parameter.setUrl(pluginConfig.getString(URL));

        if (pluginConfig.hasPath(METHOD)) {
            this.parameter.setMethod(pluginConfig.getString(METHOD));
        } else {
            this.parameter.setMethod(METHOD_DEFAULT_VALUE);
        }

        if (pluginConfig.hasPath(HEADERS)) {
            this.parameter.setHeaders(pluginConfig.getConfig(HEADERS).entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> String.valueOf(entry.getValue().unwrapped()), (v1, v2) -> v2)));
        }

        if (pluginConfig.hasPath(PARAMS)) {
            this.parameter.setHeaders(pluginConfig.getConfig(PARAMS).entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> String.valueOf(entry.getValue().unwrapped()), (v1, v2) -> v2)));
        }

        if (pluginConfig.hasPath(BODY)) {
            this.parameter.setBody(pluginConfig.getString(BODY));
        }
        // TODO support user custom row type
        this.rowType = new SeaTunnelRowType(new String[]{"content"}, new SeaTunnelDataType<?>[]{BasicType.STRING_TYPE});
    }

    @Override
    public SeaTunnelContext getSeaTunnelContext() {
        return this.seaTunnelContext;
    }

    @Override
    public void setSeaTunnelContext(SeaTunnelContext seaTunnelContext) {
        this.seaTunnelContext = seaTunnelContext;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.rowType;
    }

    @Override
    public SourceReader<SeaTunnelRow, HttpSourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new HttpSourceReader(this.parameter, readerContext);
    }

    @Override
    public SourceSplitEnumerator<HttpSourceSplit, HttpState> createEnumerator(SourceSplitEnumerator.Context<HttpSourceSplit> enumeratorContext) throws Exception {
        return new HttpSourceSplitEnumerator(enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<HttpSourceSplit, HttpState> restoreEnumerator(SourceSplitEnumerator.Context<HttpSourceSplit> enumeratorContext, HttpState checkpointState) throws Exception {
        return new HttpSourceSplitEnumerator(enumeratorContext, checkpointState);
    }

    @Override
    public Serializer<HttpState> getEnumeratorStateSerializer() {
        return new DefaultSerializer<>();
    }
}

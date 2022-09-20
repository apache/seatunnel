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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.source;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.source.SourceConfig;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

import java.util.List;

@AutoService(SeaTunnelSource.class)
public class ElasticsearchSource implements SeaTunnelSource<SeaTunnelRow, ElasticsearchSourceSplit, ElasticsearchSourceState> {


    private Config pluginConfig;

    private SeaTunnelRowType rowTypeInfo;

    @Override
    public String getPluginName() {
        return "Elasticsearch";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.pluginConfig = pluginConfig;
        List<String> source = pluginConfig.getStringList(SourceConfig.SOURCE);
        SeaTunnelDataType[] fieldTypes = new SeaTunnelDataType[source.size()];
        for (int i = 0; i < source.size(); i++) {
            fieldTypes[i] = BasicType.STRING_TYPE;
        }
        rowTypeInfo = new SeaTunnelRowType(source.toArray(new String[source.size()]), fieldTypes);
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
    public SourceReader<SeaTunnelRow, ElasticsearchSourceSplit> createReader(SourceReader.Context readerContext) {
        return new ElasticsearchSourceReader(readerContext, pluginConfig);
    }

    @Override
    public SourceSplitEnumerator<ElasticsearchSourceSplit, ElasticsearchSourceState> createEnumerator(SourceSplitEnumerator.Context<ElasticsearchSourceSplit> enumeratorContext) {
        return new ElasticsearchSourceSplitEnumerator(enumeratorContext, pluginConfig);
    }

    @Override
    public SourceSplitEnumerator<ElasticsearchSourceSplit, ElasticsearchSourceState> restoreEnumerator(SourceSplitEnumerator.Context<ElasticsearchSourceSplit> enumeratorContext, ElasticsearchSourceState checkpointState) throws Exception {
        return new ElasticsearchSourceSplitEnumerator(enumeratorContext, pluginConfig);
    }

}


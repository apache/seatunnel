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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.state.ElasticsearchAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.state.ElasticsearchCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.state.ElasticsearchSinkState;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SinkConfig.MAX_BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SinkConfig.MAX_RETRY_COUNT;

@AutoService(SeaTunnelSink.class)
public class ElasticsearchSink
        implements SeaTunnelSink<
                SeaTunnelRow,
                ElasticsearchSinkState,
                ElasticsearchCommitInfo,
                ElasticsearchAggregatedCommitInfo> {

    private Config pluginConfig;
    private SeaTunnelRowType seaTunnelRowType;

    private int maxBatchSize = MAX_BATCH_SIZE.defaultValue();

    private int maxRetryCount = MAX_RETRY_COUNT.defaultValue();

    @Override
    public String getPluginName() {
        return "Elasticsearch";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.pluginConfig = pluginConfig;
        if (pluginConfig.hasPath(MAX_BATCH_SIZE.key())) {
            maxBatchSize = pluginConfig.getInt(MAX_BATCH_SIZE.key());
        }
        if (pluginConfig.hasPath(MAX_RETRY_COUNT.key())) {
            maxRetryCount = pluginConfig.getInt(MAX_RETRY_COUNT.key());
        }
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.seaTunnelRowType;
    }

    @Override
    public SinkWriter<SeaTunnelRow, ElasticsearchCommitInfo, ElasticsearchSinkState> createWriter(
            SinkWriter.Context context) {
        return new ElasticsearchSinkWriter(
                context, seaTunnelRowType, pluginConfig, maxBatchSize, maxRetryCount);
    }
}

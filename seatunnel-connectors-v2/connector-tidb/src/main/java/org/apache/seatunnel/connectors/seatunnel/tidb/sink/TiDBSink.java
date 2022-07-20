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

package org.apache.seatunnel.connectors.seatunnel.tidb.sink;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.tidb.config.TiDBSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.tidb.connection.JdbcStatementBuilder;
import org.apache.seatunnel.connectors.seatunnel.tidb.state.JdbcAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.tidb.state.TiDBSinkState;
import org.apache.seatunnel.connectors.seatunnel.tidb.state.XidInfo;
import org.apache.seatunnel.connectors.seatunnel.tidb.utils.JdbcUtils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@AutoService(SeaTunnelSink.class)
public class TiDBSink implements SeaTunnelSink<SeaTunnelRow, TiDBSinkState, XidInfo, JdbcAggregatedCommitInfo> {

    private SeaTunnelRowType seaTunnelRowType;

    private SeaTunnelContext seaTunnelContext;

    private TiDBSinkOptions tidbSinkOptions;

    @Override
    public String getPluginName() {
        return "TiDB";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.tidbSinkOptions = new TiDBSinkOptions(pluginConfig);
    }

    @Override
    public SinkWriter<SeaTunnelRow, XidInfo, TiDBSinkState> createWriter(SinkWriter.Context context) {
        SinkWriter<SeaTunnelRow, XidInfo, TiDBSinkState> sinkWriter;
        // TODO SeatunnelTyoeInfo is not good enough to get typesArray
        JdbcStatementBuilder<SeaTunnelRow> statementBuilder = (st, row) -> JdbcUtils.setRecordToStatement(st, null, row);
        if (tidbSinkOptions.isExactlyOnce()) {
            sinkWriter = new TiDBExactlyOnceSinkWriter(
                context,
                seaTunnelContext,
                statementBuilder,
                tidbSinkOptions,
                new ArrayList<>()
            );
        } else {
            sinkWriter = new TiDBSinkWriter(
                context,
                statementBuilder,
                tidbSinkOptions.getJdbcConnectionOptions());
        }

        return sinkWriter;
    }

    @Override
    public SinkWriter<SeaTunnelRow, XidInfo, TiDBSinkState> restoreWriter(SinkWriter.Context context, List<TiDBSinkState> states)
        throws IOException {
        if (tidbSinkOptions.isExactlyOnce()) {
            JdbcStatementBuilder<SeaTunnelRow> statementBuilder = (st, row) -> JdbcUtils.setRecordToStatement(st, null, row);
            return new TiDBExactlyOnceSinkWriter(
                context,
                seaTunnelContext,
                statementBuilder,
                tidbSinkOptions,
                states
            );
        }
        return SeaTunnelSink.super.restoreWriter(context, states);
    }

    @Override
    public Optional<SinkAggregatedCommitter<XidInfo, JdbcAggregatedCommitInfo>> createAggregatedCommitter()
        throws IOException {
        if (tidbSinkOptions.isExactlyOnce()) {
            return Optional.of(new TiDBSinkAggregatedCommitter(tidbSinkOptions.getJdbcConnectionOptions()));
        }
        return Optional.empty();
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
    public Optional<Serializer<JdbcAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public void setSeaTunnelContext(SeaTunnelContext seaTunnelContext) {
        this.seaTunnelContext = seaTunnelContext;
    }

    @Override
    public Optional<Serializer<XidInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }
}

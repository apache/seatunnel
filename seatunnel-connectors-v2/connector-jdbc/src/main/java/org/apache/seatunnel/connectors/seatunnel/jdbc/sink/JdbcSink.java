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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@AutoService(SeaTunnelSink.class)
public class JdbcSink
    implements SeaTunnelSink<SeaTunnelRow, JdbcSinkState, XidInfo, JdbcAggregatedCommitInfo> {

    private Config pluginConfig;

    private SeaTunnelRowType seaTunnelRowType;

    private JobContext jobContext;

    private JdbcSinkOptions jdbcSinkOptions;

    private JdbcDialect dialect;

    @Override
    public String getPluginName() {
        return "Jdbc";
    }

    @Override
    public void prepare(Config pluginConfig)
        throws PrepareFailException {
        this.pluginConfig = pluginConfig;
        this.jdbcSinkOptions = new JdbcSinkOptions(this.pluginConfig);
        this.dialect = JdbcDialectLoader.load(jdbcSinkOptions.getJdbcConnectionOptions().getUrl());
    }

    @Override
    public SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> createWriter(SinkWriter.Context context)
        throws IOException {
        SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> sinkWriter;
        if (jdbcSinkOptions.isExactlyOnce()) {
            sinkWriter = new JdbcExactlyOnceSinkWriter(
                context,
                jobContext,
                dialect,
                jdbcSinkOptions,
                seaTunnelRowType,
                new ArrayList<>()
            );
        } else {
            sinkWriter = new JdbcSinkWriter(
                context,
                dialect,
                jdbcSinkOptions,
                seaTunnelRowType);
        }

        return sinkWriter;
    }

    @Override
    public SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> restoreWriter(SinkWriter.Context context, List<JdbcSinkState> states)
        throws IOException {
        if (jdbcSinkOptions.isExactlyOnce()) {
            return new JdbcExactlyOnceSinkWriter(
                context,
                jobContext,
                dialect,
                jdbcSinkOptions,
                seaTunnelRowType,
                states
            );
        }
        return SeaTunnelSink.super.restoreWriter(context, states);
    }

    @Override
    public Optional<SinkAggregatedCommitter<XidInfo, JdbcAggregatedCommitInfo>> createAggregatedCommitter()
        throws IOException {
        if (jdbcSinkOptions.isExactlyOnce()) {
            return Optional.of(new JdbcSinkAggregatedCommitter(jdbcSinkOptions));
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
        if (jdbcSinkOptions.isExactlyOnce()) {
            return Optional.of(new DefaultSerializer<>());
        }
        return Optional.empty();

    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public Optional<Serializer<XidInfo>> getCommitInfoSerializer() {
        if (jdbcSinkOptions.isExactlyOnce()) {
            return Optional.of(new DefaultSerializer<>());
        }
        return Optional.empty();
    }
}

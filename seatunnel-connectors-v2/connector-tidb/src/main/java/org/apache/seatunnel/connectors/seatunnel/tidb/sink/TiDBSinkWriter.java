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

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.tidb.config.JdbcConnectionOptions;
import org.apache.seatunnel.connectors.seatunnel.tidb.connection.JdbcBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.tidb.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.tidb.connection.JdbcOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.tidb.connection.JdbcStatementBuilder;
import org.apache.seatunnel.connectors.seatunnel.tidb.connection.SimpleBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.tidb.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.tidb.state.TiDBSinkState;
import org.apache.seatunnel.connectors.seatunnel.tidb.state.XidInfo;

import org.apache.commons.lang3.SerializationUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class TiDBSinkWriter implements SinkWriter<SeaTunnelRow, XidInfo, TiDBSinkState> {

    private final JdbcOutputFormat<SeaTunnelRow, JdbcBatchStatementExecutor<SeaTunnelRow>> outputFormat;
    private transient boolean isOpen;

    public TiDBSinkWriter(
        SinkWriter.Context context,
        JdbcStatementBuilder<SeaTunnelRow> statementBuilder,
        JdbcConnectionOptions jdbcConnectionOptions) {

        JdbcConnectionProvider connectionProvider = new SimpleJdbcConnectionProvider(jdbcConnectionOptions);

        this.outputFormat = new JdbcOutputFormat<>(
            connectionProvider,
            jdbcConnectionOptions,
            () -> new SimpleBatchStatementExecutor<>(jdbcConnectionOptions.getQuery(), statementBuilder));
    }

    private void tryOpen() throws IOException {
        if (!isOpen) {
            isOpen = true;
            outputFormat.open();
        }
    }

    @Override
    public List<TiDBSinkState> snapshotState(long checkpointId) {
        return Collections.emptyList();
    }

    @Override
    public void write(SeaTunnelRow element)
        throws IOException {
        tryOpen();
        SeaTunnelRow copy = SerializationUtils.clone(element);
        outputFormat.writeRecord(copy);
    }

    @Override
    public Optional<XidInfo> prepareCommit()
        throws IOException {
        outputFormat.flush();
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {

    }

    @Override
    public void close()
        throws IOException {
        outputFormat.close();
    }
}

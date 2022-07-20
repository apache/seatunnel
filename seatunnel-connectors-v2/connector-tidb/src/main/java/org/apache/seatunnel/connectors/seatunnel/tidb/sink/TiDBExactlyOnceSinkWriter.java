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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.tidb.config.TiDBSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.tidb.connection.JdbcBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.tidb.connection.JdbcOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.tidb.connection.JdbcStatementBuilder;
import org.apache.seatunnel.connectors.seatunnel.tidb.connection.SimpleBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.tidb.sink.xa.XaFacade;
import org.apache.seatunnel.connectors.seatunnel.tidb.sink.xa.XaGroupOps;
import org.apache.seatunnel.connectors.seatunnel.tidb.sink.xa.XaGroupOpsImpl;
import org.apache.seatunnel.connectors.seatunnel.tidb.sink.xa.XidGenerator;
import org.apache.seatunnel.connectors.seatunnel.tidb.state.TiDBSinkState;
import org.apache.seatunnel.connectors.seatunnel.tidb.state.XidInfo;
import org.apache.seatunnel.connectors.seatunnel.tidb.utils.ExceptionUtils;

import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.Xid;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class TiDBExactlyOnceSinkWriter
    implements SinkWriter<SeaTunnelRow, XidInfo, TiDBSinkState> {
    private static final Logger LOG = LoggerFactory.getLogger(TiDBExactlyOnceSinkWriter.class);

    private final Context sinkcontext;

    private final SeaTunnelContext context;

    private final List<TiDBSinkState> recoverStates;

    private final XaFacade xaFacade;

    private final XaGroupOps xaGroupOps;

    private final XidGenerator xidGenerator;

    private final JdbcOutputFormat<SeaTunnelRow, JdbcBatchStatementExecutor<SeaTunnelRow>> outputFormat;

    private transient boolean isOpen;

    private transient Xid currentXid;
    private transient Xid prepareXid;

    public TiDBExactlyOnceSinkWriter(
        SinkWriter.Context sinkcontext,
        SeaTunnelContext context,
        JdbcStatementBuilder<SeaTunnelRow> statementBuilder,
        TiDBSinkOptions tidbSinkOptions,
        List<TiDBSinkState> states) {
        checkArgument(
            tidbSinkOptions.getJdbcConnectionOptions().getMaxRetries() == 0,
            "JDBC XA sink requires maxRetries equal to 0, otherwise it could "
                + "cause duplicates.");

        this.context = context;
        this.sinkcontext = sinkcontext;
        this.recoverStates = states;
        this.xidGenerator = XidGenerator.semanticXidGenerator();
        checkState(tidbSinkOptions.isExactlyOnce(), "is_exactly_once config error");
        this.xaFacade = XaFacade.fromJdbcConnectionOptions(
            tidbSinkOptions.getJdbcConnectionOptions());

        this.outputFormat = new JdbcOutputFormat<>(
            xaFacade,
            tidbSinkOptions.getJdbcConnectionOptions(),
            () -> new SimpleBatchStatementExecutor<>(tidbSinkOptions.getJdbcConnectionOptions().getQuery(), statementBuilder));

        this.xaGroupOps = new XaGroupOpsImpl(xaFacade);
    }

    private void tryOpen() throws IOException {
        if (!isOpen) {
            isOpen = true;
            try {
                xidGenerator.open();
                xaFacade.open();
                outputFormat.open();
                if (!recoverStates.isEmpty()) {
                    Xid xid = recoverStates.get(0).getXid();
                    // Rollback pending transactions that should not include recoverStates
                    xaGroupOps.recoverAndRollback(context, sinkcontext, xidGenerator, xid);
                }
                beginTx();
            } catch (Exception e) {
                ExceptionUtils.rethrowIOException(e);
            }
        }
    }

    @Override
    public List<TiDBSinkState> snapshotState(long checkpointId) {
        checkState(prepareXid != null, "prepare xid must not be null");
        return Collections.singletonList(new TiDBSinkState(prepareXid));
    }

    @Override
    public void write(SeaTunnelRow element)
        throws IOException {
        tryOpen();
        checkState(currentXid != null, "current xid must not be null");
        SeaTunnelRow copy = SerializationUtils.clone(element);
        outputFormat.writeRecord(copy);
    }

    @Override
    public Optional<XidInfo> prepareCommit()
        throws IOException {
        prepareCurrentTx();
        this.currentXid = null;
        beginTx();
        checkState(prepareXid != null, "prepare xid must not be null");
        return Optional.of(new XidInfo(prepareXid, 0));
    }

    @Override
    public void abortPrepare() {

    }

    @Override
    public void close()
        throws IOException {
        if (currentXid != null && xaFacade.isOpen()) {
            try {
                LOG.debug("remove current transaction before closing, xid={}", currentXid);
                xaFacade.failAndRollback(currentXid);
            } catch (Exception e) {
                LOG.warn("unable to fail/rollback current transaction, xid={}", currentXid, e);
            }
        }
        try {
            xaFacade.close();
        } catch (Exception e) {
            ExceptionUtils.rethrowIOException(e);
        }
        xidGenerator.close();
        currentXid = null;
        prepareXid = null;
    }

    private void beginTx() throws IOException {
        checkState(currentXid == null, "currentXid not null");
        currentXid = xidGenerator.generateXid(context, sinkcontext, System.currentTimeMillis());
        try {
            xaFacade.start(currentXid);
        } catch (Exception e) {
            ExceptionUtils.rethrowIOException(e);
        }
    }

    private void prepareCurrentTx() throws IOException {
        checkState(currentXid != null, "no current xid");
        outputFormat.flush();
        try {
            xaFacade.endAndPrepare(currentXid);
            prepareXid = currentXid;
        } catch (Exception e) {
            ExceptionUtils.rethrowIOException(e);
        }
    }
}

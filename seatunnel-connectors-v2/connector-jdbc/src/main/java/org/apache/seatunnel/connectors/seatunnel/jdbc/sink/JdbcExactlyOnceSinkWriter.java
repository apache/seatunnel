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
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormatBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa.XaFacade;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa.XaGroupOps;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa.XaGroupOpsImpl;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa.XidGenerator;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import org.apache.commons.lang3.SerializationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import javax.transaction.xa.Xid;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkState;

public class JdbcExactlyOnceSinkWriter
        implements SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState>,
                SupportMultiTableSinkWriter<Void> {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcExactlyOnceSinkWriter.class);

    private final SinkWriter.Context sinkcontext;

    private final JobContext context;

    private final List<JdbcSinkState> recoverStates;

    private final XaFacade xaFacade;

    private final XaGroupOps xaGroupOps;

    private final XidGenerator xidGenerator;

    private final JdbcOutputFormat<SeaTunnelRow, JdbcBatchStatementExecutor<SeaTunnelRow>>
            outputFormat;

    private transient boolean isOpen;

    private transient Xid currentXid;
    private transient Xid prepareXid;

    public JdbcExactlyOnceSinkWriter(
            SinkWriter.Context sinkcontext,
            JobContext context,
            JdbcDialect dialect,
            JdbcSinkConfig jdbcSinkConfig,
            SeaTunnelRowType rowType,
            List<JdbcSinkState> states) {
        checkArgument(
                jdbcSinkConfig.getJdbcConnectionConfig().getMaxRetries() == 0,
                "JDBC XA sink requires maxRetries equal to 0, otherwise it could "
                        + "cause duplicates.");

        this.context = context;
        this.sinkcontext = sinkcontext;
        this.recoverStates = states;
        this.xidGenerator = XidGenerator.semanticXidGenerator();
        checkState(jdbcSinkConfig.isExactlyOnce(), "is_exactly_once config error");
        this.xaFacade =
                XaFacade.fromJdbcConnectionOptions(jdbcSinkConfig.getJdbcConnectionConfig());
        this.outputFormat =
                new JdbcOutputFormatBuilder(dialect, xaFacade, jdbcSinkConfig, rowType).build();
        this.xaGroupOps = new XaGroupOpsImpl(xaFacade);
    }

    private void tryOpen() {
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
                throw new JdbcConnectorException(
                        CommonErrorCode.WRITER_OPERATION_FAILED,
                        "unable to open JDBC exactly one writer",
                        e);
            }
        }
    }

    @Override
    public List<JdbcSinkState> snapshotState(long checkpointId) {
        checkState(prepareXid != null, "prepare xid must not be null");
        return Collections.singletonList(new JdbcSinkState(prepareXid));
    }

    @Override
    public void write(SeaTunnelRow element) {
        tryOpen();
        checkState(currentXid != null, "current xid must not be null");
        SeaTunnelRow copy = SerializationUtils.clone(element);
        outputFormat.writeRecord(copy);
    }

    @Override
    public Optional<XidInfo> prepareCommit() throws IOException {
        tryOpen();

        boolean emptyXaTransaction = false;
        try {
            prepareCurrentTx();
        } catch (Exception e) {
            if (Throwables.getRootCause(e) instanceof XaFacade.EmptyXaTransactionException) {
                emptyXaTransaction = true;
                LOG.info("skip prepare empty xa transaction, xid={}", currentXid);
            } else {
                throw e;
            }
        }
        this.currentXid = null;
        beginTx();
        checkState(prepareXid != null, "prepare xid must not be null");
        return emptyXaTransaction ? Optional.empty() : Optional.of(new XidInfo(prepareXid, 0));
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
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
            throw new JdbcConnectorException(
                    CommonErrorCode.WRITER_OPERATION_FAILED,
                    "unable to close JDBC exactly one writer",
                    e);
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
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.XA_OPERATION_FAILED,
                    "unable to start xa transaction",
                    e);
        }
    }

    private void prepareCurrentTx() throws IOException {
        checkState(currentXid != null, "no current xid");
        outputFormat.flush();

        Exception endAndPrepareException = null;
        try {
            xaFacade.endAndPrepare(currentXid);
        } catch (Exception e) {
            endAndPrepareException = e;
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.XA_OPERATION_FAILED,
                    "unable to prepare current xa transaction",
                    e);
        } finally {
            if (endAndPrepareException == null
                    || Throwables.getRootCause(endAndPrepareException)
                            instanceof XaFacade.EmptyXaTransactionException) {
                prepareXid = currentXid;
            }
        }
    }
}

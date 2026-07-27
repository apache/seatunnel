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

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa.GroupXaOperationResult;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa.XaFacade;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa.XaGroupOps;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa.XaGroupOpsImpl;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import lombok.extern.slf4j.Slf4j;

import javax.transaction.xa.Xid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Slf4j
public class JdbcSinkAggregatedCommitter
        implements SinkAggregatedCommitter<XidInfo, JdbcAggregatedCommitInfo> {

    private XaFacade xaFacade;
    private XaGroupOps xaGroupOps;
    private final JdbcSinkConfig jdbcSinkConfig;

    public JdbcSinkAggregatedCommitter(JdbcSinkConfig jdbcSinkConfig) {
        this.jdbcSinkConfig = jdbcSinkConfig;
    }

    @Override
    public void init() {
        this.xaFacade =
                XaFacade.fromJdbcConnectionOptions(jdbcSinkConfig.getJdbcConnectionConfig());
        this.xaGroupOps = new XaGroupOpsImpl(xaFacade);
    }

    private void tryOpen() throws IOException {
        if (!xaFacade.isOpen()) {
            try {
                xaFacade.open();
            } catch (Exception e) {
                throw new JdbcConnectorException(
                        CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                        "unable to open JDBC sink aggregated committer",
                        e);
            }
        }
    }

    @Override
    public List<JdbcAggregatedCommitInfo> commit(
            List<JdbcAggregatedCommitInfo> aggregatedCommitInfos) throws IOException {
        return commitPreparedTransactions(aggregatedCommitInfos);
    }

    /**
     * Reconciles checkpoint XIDs with the resource manager and replays only checkpoint-owned
     * transactions. An XID missing from the recovery scan is committed strictly so XAER_NOTA or
     * another unknown outcome fails recovery instead of being inferred as success.
     */
    @Override
    public List<JdbcAggregatedCommitInfo> restoreCommit(
            List<JdbcAggregatedCommitInfo> aggregatedCommitInfos) throws IOException {
        tryOpen();
        Collection<Xid> recoveredXids = xaFacade.recover();
        for (JdbcAggregatedCommitInfo aggregatedCommitInfo : aggregatedCommitInfos) {
            for (XidInfo xidInfo : aggregatedCommitInfo.getXidInfoList()) {
                if (!containsEquivalentXid(recoveredXids, xidInfo.getXid())) {
                    log.warn(
                            "Checkpoint transaction {} is absent from the XA recovery scan; "
                                    + "requiring an explicit resource-manager result",
                            xidInfo.getXid());
                }
            }
        }
        return commitPreparedTransactions(aggregatedCommitInfos);
    }

    /**
     * Commits prepared transactions and completes bounded transient retries in the same invocation.
     *
     * @param aggregatedCommitInfos prepared transactions grouped for commit
     * @return an empty list after all prepared transactions have been committed
     * @throws IOException when the committer cannot open its XA resource
     */
    private List<JdbcAggregatedCommitInfo> commitPreparedTransactions(
            List<JdbcAggregatedCommitInfo> aggregatedCommitInfos) throws IOException {
        tryOpen();
        for (JdbcAggregatedCommitInfo aggregatedCommitInfo : aggregatedCommitInfos) {
            log.info("commit xid: " + aggregatedCommitInfo.getXidInfoList());
            List<XidInfo> pending = new ArrayList<>(aggregatedCommitInfo.getXidInfoList());
            while (!pending.isEmpty()) {
                GroupXaOperationResult<XidInfo> result =
                        xaGroupOps.commit(
                                pending,
                                false,
                                jdbcSinkConfig.getJdbcConnectionConfig().getMaxCommitAttempts());
                // Zeta does not persist the returned committables before restarting a failed
                // checkpoint, so complete bounded retries in this invocation.
                pending = new ArrayList<>(result.getForRetry());
            }
        }
        return Collections.emptyList();
    }

    @Override
    public JdbcAggregatedCommitInfo combine(List<XidInfo> commitInfos) {
        return new JdbcAggregatedCommitInfo(commitInfos);
    }

    @Override
    public void abort(List<JdbcAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        tryOpen();
        for (JdbcAggregatedCommitInfo commitInfos : aggregatedCommitInfo) {
            xaGroupOps.rollback(commitInfos.getXidInfoList());
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (xaFacade.isOpen()) {
                xaFacade.close();
            }
        } catch (Exception e) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                    "unable to close JDBC sink aggregated committer",
                    e);
        }
    }

    /**
     * Compares canonical XID components so driver-specific {@link Xid} implementations do not
     * affect recovery reconciliation.
     *
     * @param recoveredXids transactions returned by the resource manager
     * @param checkpointXid transaction restored from checkpoint state
     * @return whether the recovery scan contains the checkpoint transaction
     */
    private boolean containsEquivalentXid(Collection<Xid> recoveredXids, Xid checkpointXid) {
        return recoveredXids.stream()
                .anyMatch(
                        recoveredXid ->
                                recoveredXid.getFormatId() == checkpointXid.getFormatId()
                                        && Arrays.equals(
                                                recoveredXid.getGlobalTransactionId(),
                                                checkpointXid.getGlobalTransactionId())
                                        && Arrays.equals(
                                                recoveredXid.getBranchQualifier(),
                                                checkpointXid.getBranchQualifier()));
    }
}

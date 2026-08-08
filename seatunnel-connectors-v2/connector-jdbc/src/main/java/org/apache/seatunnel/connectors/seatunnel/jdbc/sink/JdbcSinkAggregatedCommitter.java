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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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
     * transactions. XIDs still present in the recovery scan are committed strictly. XIDs absent
     * from the recovery scan are replayed with XAER_NOTA tolerance because a previous aborted
     * restore attempt may have already committed them before failing on a later XID.
     */
    @Override
    public List<JdbcAggregatedCommitInfo> restoreCommit(
            List<JdbcAggregatedCommitInfo> aggregatedCommitInfos) throws IOException {
        tryOpen();
        Set<XidKey> recoveredXids = normalizeXids(xaFacade.recover());
        for (JdbcAggregatedCommitInfo aggregatedCommitInfo : aggregatedCommitInfos) {
            List<XidInfo> recovered = new ArrayList<>();
            List<XidInfo> alreadyResolved = new ArrayList<>();
            for (XidInfo xidInfo : aggregatedCommitInfo.getXidInfoList()) {
                if (containsEquivalentXid(recoveredXids, xidInfo.getXid())) {
                    recovered.add(xidInfo);
                } else {
                    log.warn(
                            "Checkpoint transaction {} is absent from the XA recovery scan; "
                                    + "allowing XAER_NOTA only for this restore replay",
                            xidInfo.getXid());
                    alreadyResolved.add(xidInfo);
                }
            }
            commitXidInfos(recovered, false);
            commitXidInfos(alreadyResolved, true);
        }
        return Collections.emptyList();
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
            commitXidInfos(aggregatedCommitInfo.getXidInfoList(), false);
        }
        return Collections.emptyList();
    }

    /**
     * Commits one XID list and exhausts bounded transient retries while the retry-attempt state is
     * still available in memory.
     *
     * @param xidInfos prepared transactions to commit
     * @param ignoreUnknown whether XAER_NOTA is accepted as an idempotent restore replay result
     */
    private void commitXidInfos(List<XidInfo> xidInfos, boolean ignoreUnknown) {
        List<XidInfo> pending = new ArrayList<>(xidInfos);
        while (!pending.isEmpty()) {
            GroupXaOperationResult<XidInfo> result =
                    xaGroupOps.commit(
                            pending,
                            false,
                            jdbcSinkConfig.getJdbcConnectionConfig().getMaxCommitAttempts(),
                            ignoreUnknown);
            // Zeta does not persist the returned committables before restarting a failed
            // checkpoint, so complete bounded retries in this invocation.
            pending = new ArrayList<>(result.getForRetry());
        }
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
     * Normalizes driver-specific {@link Xid} implementations into canonical values for recovery
     * reconciliation.
     *
     * @param recoveredXids transactions returned by the resource manager
     * @return canonical values for transactions returned by the resource manager
     */
    private Set<XidKey> normalizeXids(Collection<Xid> recoveredXids) {
        Set<XidKey> normalized = new HashSet<>();
        for (Xid xid : recoveredXids) {
            normalized.add(XidKey.from(xid));
        }
        return normalized;
    }

    /**
     * Checks whether the recovery scan contains the checkpoint transaction by canonical XID value.
     *
     * @param recoveredXids canonical recovery-scan values
     * @param checkpointXid transaction restored from checkpoint state
     * @return whether the recovery scan contains the checkpoint transaction
     */
    private boolean containsEquivalentXid(Set<XidKey> recoveredXids, Xid checkpointXid) {
        return recoveredXids.contains(XidKey.from(checkpointXid));
    }

    /** Canonical XID value used to compare driver-specific {@link Xid} implementations. */
    private static final class XidKey {
        private final int formatId;
        private final byte[] globalTransactionId;
        private final byte[] branchQualifier;

        private XidKey(int formatId, byte[] globalTransactionId, byte[] branchQualifier) {
            this.formatId = formatId;
            this.globalTransactionId =
                    Arrays.copyOf(globalTransactionId, globalTransactionId.length);
            this.branchQualifier = Arrays.copyOf(branchQualifier, branchQualifier.length);
        }

        private static XidKey from(Xid xid) {
            return new XidKey(
                    xid.getFormatId(), xid.getGlobalTransactionId(), xid.getBranchQualifier());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof XidKey)) {
                return false;
            }
            XidKey xidKey = (XidKey) o;
            return formatId == xidKey.formatId
                    && Arrays.equals(globalTransactionId, xidKey.globalTransactionId)
                    && Arrays.equals(branchQualifier, xidKey.branchQualifier);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(formatId);
            result = 31 * result + Arrays.hashCode(globalTransactionId);
            result = 31 * result + Arrays.hashCode(branchQualifier);
            return result;
        }
    }
}

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
import java.util.concurrent.TimeUnit;

@Slf4j
public class JdbcSinkAggregatedCommitter
        implements SinkAggregatedCommitter<XidInfo, JdbcAggregatedCommitInfo> {

    private static final long XA_RETRY_BACKOFF_MILLIS = TimeUnit.SECONDS.toMillis(1);

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
     * Reconciles checkpoint XIDs with the resource manager using commit-order evidence. Checkpoint
     * XIDs from the first still-prepared transaction onward must all be present in the recovery
     * scan and are replayed strictly. An all-absent batch is treated as already resolved, while an
     * absent prefix before a still-prepared suffix is treated as already resolved only after that
     * suffix commits successfully. The scan is refreshed for each batch because an external
     * resource-manager actor, such as a DBA or RM cleanup process, can resolve an in-doubt
     * transaction between restored batches. This is not a second Zeta committer; the refresh
     * narrows, but cannot eliminate, the gap before the following XA commit call.
     */
    @Override
    public List<JdbcAggregatedCommitInfo> restoreCommit(
            List<JdbcAggregatedCommitInfo> aggregatedCommitInfos) throws IOException {
        tryOpen();
        for (JdbcAggregatedCommitInfo aggregatedCommitInfo : aggregatedCommitInfos) {
            // Refresh RM evidence for every batch because a DBA or RM cleanup process can resolve
            // an in-doubt transaction while earlier restored batches are being replayed.
            replayRecoveredCheckpoint(
                    aggregatedCommitInfo.getXidInfoList(), recoverCheckpointTransactions());
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
            commitXidInfos(aggregatedCommitInfo.getXidInfoList());
        }
        return Collections.emptyList();
    }

    /**
     * Commits one XID list and exhausts bounded transient retries while the retry-attempt state is
     * still available in memory.
     *
     * @param xidInfos prepared transactions to commit
     */
    private void commitXidInfos(List<XidInfo> xidInfos) {
        List<XidInfo> pending = new ArrayList<>(xidInfos);
        int maxCommitAttempts = jdbcSinkConfig.getJdbcConnectionConfig().getMaxCommitAttempts();
        int remainingRounds = Math.max(1, maxCommitAttempts);
        while (!pending.isEmpty()) {
            if (remainingRounds-- == 0) {
                throw new JdbcConnectorException(
                        CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                        String.format(
                                "XA commit retry loop did not terminate within %d rounds for transactions: %s",
                                Math.max(1, maxCommitAttempts), pending));
            }
            GroupXaOperationResult<XidInfo> result =
                    xaGroupOps.commit(pending, false, maxCommitAttempts);
            // Zeta does not persist the returned committables before restarting a failed
            // checkpoint, so complete bounded retries in this invocation.
            pending = new ArrayList<>(result.getForRetry());
            if (!pending.isEmpty() && remainingRounds > 0) {
                backoffBeforeRetry(
                        "commit",
                        Math.max(1, maxCommitAttempts) - remainingRounds + 1,
                        Math.max(1, maxCommitAttempts));
            }
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
     * Replays one restored checkpoint using the recovery scan as evidence for which transactions
     * can still be committed safely.
     *
     * @param checkpointXids checkpoint-owned prepared transactions in original commit order
     * @param recoveredXids canonical values returned by the resource manager recovery scan
     */
    private void replayRecoveredCheckpoint(
            List<XidInfo> checkpointXids, Set<XidKey> recoveredXids) {
        if (checkpointXids.isEmpty()) {
            return;
        }
        int firstRecoveredIndex = findFirstRecoveredIndex(checkpointXids, recoveredXids);
        if (firstRecoveredIndex < 0) {
            log.warn(
                    "Skipping checkpoint batch because none of its transactions remain in the XA recovery scan; treating it as already resolved: {}",
                    checkpointXids);
            return;
        }
        List<XidInfo> stillPrepared = new ArrayList<>();
        for (int i = firstRecoveredIndex; i < checkpointXids.size(); i++) {
            XidInfo xidInfo = checkpointXids.get(i);
            if (!containsEquivalentXid(recoveredXids, xidInfo.getXid())) {
                throw new JdbcConnectorException(
                        CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                        String.format(
                                "checkpoint transaction %s is absent from the XA recovery scan after still-prepared transactions in the same commit batch: %s",
                                xidInfo.getXid(), checkpointXids));
            }
            stillPrepared.add(xidInfo);
        }
        commitXidInfos(stillPrepared);
        if (firstRecoveredIndex > 0) {
            List<XidInfo> alreadyResolved = checkpointXids.subList(0, firstRecoveredIndex);
            log.warn(
                    "Skipping {} checkpoint transactions that are absent from the XA recovery scan but precede the first still-prepared transaction: {}",
                    alreadyResolved.size(),
                    alreadyResolved);
        }
    }

    /**
     * Retries the recovery scan with the same bounded budget as XA commit so transient RM outages
     * do not fail restore immediately.
     *
     * @return canonical values returned by the recovery scan
     */
    private Set<XidKey> recoverCheckpointTransactions() {
        int maxCommitAttempts = jdbcSinkConfig.getJdbcConnectionConfig().getMaxCommitAttempts();
        XaFacade.TransientXaException lastTransientFailure = null;
        for (int attempt = 1; attempt <= Math.max(1, maxCommitAttempts); attempt++) {
            try {
                return normalizeXids(xaFacade.recover());
            } catch (XaFacade.TransientXaException e) {
                lastTransientFailure = e;
                log.warn(
                        "Transient XA recovery-scan failure on attempt {}/{}",
                        attempt,
                        Math.max(1, maxCommitAttempts),
                        e);
                if (attempt < Math.max(1, maxCommitAttempts)) {
                    backoffBeforeRetry(
                            "recovery scan", attempt + 1, Math.max(1, maxCommitAttempts));
                }
            }
        }
        throw new JdbcConnectorException(
                CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                String.format(
                        "unable to complete the XA recovery scan within %d attempts",
                        Math.max(1, maxCommitAttempts)),
                lastTransientFailure);
    }

    /**
     * Finds the first checkpoint transaction that is still prepared in the resource manager.
     *
     * @param checkpointXids checkpoint-owned prepared transactions in original commit order
     * @param recoveredXids canonical values returned by the recovery scan
     * @return the first still-prepared checkpoint transaction index, or {@code -1} if none remain
     */
    private int findFirstRecoveredIndex(List<XidInfo> checkpointXids, Set<XidKey> recoveredXids) {
        for (int i = 0; i < checkpointXids.size(); i++) {
            if (containsEquivalentXid(recoveredXids, checkpointXids.get(i).getXid())) {
                return i;
            }
        }
        return -1;
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

    /**
     * Adds a bounded pause between synchronous retry rounds so transient RM outages do not consume
     * the whole retry budget immediately.
     *
     * @param operation operation being retried
     * @param nextAttempt 1-based retry attempt number that will run after the pause
     * @param maxAttempts bounded retry budget
     */
    private void backoffBeforeRetry(String operation, int nextAttempt, int maxAttempts) {
        try {
            Thread.sleep(XA_RETRY_BACKOFF_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                    String.format(
                            "Interrupted while waiting to retry XA %s on attempt %d/%d",
                            operation, nextAttempt, maxAttempts),
                    e);
        }
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

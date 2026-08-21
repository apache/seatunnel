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

import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa.GroupXaOperationResult;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa.XaFacade;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa.XaGroupOps;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests normal and recovery commit semantics, including RM reconciliation, for the JDBC aggregated
 * committer.
 */
class JdbcSinkAggregatedCommitterTest {

    /**
     * Verifies that recovery matches driver-specific XIDs by value and never commits an unrelated
     * recovered transaction.
     */
    @Test
    void testRestoreCommitReconcilesRecoveredTransactionsByValue() throws Exception {
        JdbcConnectionConfig connectionConfig =
                JdbcConnectionConfig.builder().maxCommitAttempts(3).build();
        JdbcSinkConfig sinkConfig =
                JdbcSinkConfig.builder().jdbcConnectionConfig(connectionConfig).build();
        JdbcSinkAggregatedCommitter committer = new JdbcSinkAggregatedCommitter(sinkConfig);
        XaFacade xaFacade = mock(XaFacade.class);
        XaGroupOps xaGroupOps = mock(XaGroupOps.class);
        when(xaFacade.isOpen()).thenReturn(true);
        Xid checkpointXid = createXid(1, new byte[] {1}, new byte[] {2});
        Xid recoveredXid = createXid(1, new byte[] {1}, new byte[] {2});
        Xid unrelatedXid = createXid(2, new byte[] {3}, new byte[] {4});
        when(xaFacade.recover()).thenReturn(Arrays.asList(recoveredXid, unrelatedXid));
        when(xaGroupOps.commit(anyList(), eq(false), eq(3)))
                .thenReturn(new GroupXaOperationResult<>());
        setPrivateField(committer, "xaFacade", xaFacade);
        setPrivateField(committer, "xaGroupOps", xaGroupOps);
        JdbcAggregatedCommitInfo commitInfo =
                new JdbcAggregatedCommitInfo(
                        Collections.singletonList(new XidInfo(checkpointXid, 0)));

        committer.restoreCommit(Collections.singletonList(commitInfo));

        verify(xaFacade).recover();
        verify(xaGroupOps)
                .commit(
                        argThat(xids -> xids.size() == 1 && xids.get(0).getXid() == checkpointXid),
                        eq(false),
                        eq(3));
    }

    /**
     * Verifies that each restored batch uses a fresh recovery scan instead of reusing stale
     * resource-manager evidence from an earlier batch.
     */
    @Test
    void testRestoreCommitRefreshesRecoveryScanForEachBatch() throws Exception {
        JdbcSinkAggregatedCommitter committer = createCommitter();
        XaFacade xaFacade = mock(XaFacade.class);
        XaGroupOps xaGroupOps = mock(XaGroupOps.class);
        Xid firstCheckpointXid = createXid(1, new byte[] {1}, new byte[] {1});
        Xid secondCheckpointXid = createXid(2, new byte[] {2}, new byte[] {2});
        Xid firstRecoveredXid = createXid(1, new byte[] {1}, new byte[] {1});
        Xid secondRecoveredXid = createXid(2, new byte[] {2}, new byte[] {2});
        when(xaFacade.isOpen()).thenReturn(true);
        when(xaFacade.recover())
                .thenReturn(Arrays.asList(firstRecoveredXid, secondRecoveredXid))
                .thenReturn(Collections.emptyList());
        when(xaGroupOps.commit(anyList(), eq(false), eq(3)))
                .thenReturn(new GroupXaOperationResult<>());
        setPrivateField(committer, "xaFacade", xaFacade);
        setPrivateField(committer, "xaGroupOps", xaGroupOps);
        JdbcAggregatedCommitInfo firstCommitInfo =
                new JdbcAggregatedCommitInfo(
                        Collections.singletonList(new XidInfo(firstCheckpointXid, 0)));
        JdbcAggregatedCommitInfo secondCommitInfo =
                new JdbcAggregatedCommitInfo(
                        Collections.singletonList(new XidInfo(secondCheckpointXid, 0)));

        committer.restoreCommit(Arrays.asList(firstCommitInfo, secondCommitInfo));

        verify(xaFacade, times(2)).recover();
        verify(xaGroupOps)
                .commit(
                        argThat(
                                xids ->
                                        xids.size() == 1
                                                && xids.get(0).getXid() == firstCheckpointXid),
                        eq(false),
                        eq(3));
        verify(xaGroupOps, never())
                .commit(
                        argThat(
                                xids ->
                                        xids.stream()
                                                .anyMatch(
                                                        xidInfo ->
                                                                xidInfo.getXid()
                                                                        == secondCheckpointXid)),
                        eq(false),
                        eq(3));
    }

    /**
     * Verifies that a missing prefix is skipped only after the still-prepared suffix commits
     * strictly.
     */
    @Test
    void testRestoreCommitSkipsMissingPrefixAfterRecoveredSuffixCommits() throws Exception {
        JdbcSinkAggregatedCommitter committer = createCommitter();
        XaFacade xaFacade = mock(XaFacade.class);
        XaGroupOps xaGroupOps = mock(XaGroupOps.class);
        Xid alreadyResolvedXid = createXid(1, new byte[] {1}, new byte[] {1});
        Xid stillPreparedXid = createXid(2, new byte[] {2}, new byte[] {2});
        Xid recoveredStillPreparedXid = createXid(2, new byte[] {2}, new byte[] {2});
        when(xaFacade.isOpen()).thenReturn(true);
        when(xaFacade.recover()).thenReturn(Collections.singletonList(recoveredStillPreparedXid));
        when(xaGroupOps.commit(anyList(), eq(false), eq(3)))
                .thenReturn(new GroupXaOperationResult<>());
        setPrivateField(committer, "xaFacade", xaFacade);
        setPrivateField(committer, "xaGroupOps", xaGroupOps);
        JdbcAggregatedCommitInfo commitInfo =
                new JdbcAggregatedCommitInfo(
                        Arrays.asList(
                                new XidInfo(alreadyResolvedXid, 0),
                                new XidInfo(stillPreparedXid, 0)));

        Assertions.assertDoesNotThrow(
                () -> committer.restoreCommit(Collections.singletonList(commitInfo)));

        verify(xaGroupOps)
                .commit(
                        argThat(
                                xids ->
                                        xids.size() == 1
                                                && xids.get(0).getXid() == stillPreparedXid),
                        eq(false),
                        eq(3));
        verify(xaGroupOps, never())
                .commit(
                        argThat(
                                xids ->
                                        xids.stream()
                                                .anyMatch(
                                                        xidInfo ->
                                                                xidInfo.getXid()
                                                                        == alreadyResolvedXid)),
                        eq(false),
                        eq(3));
    }

    /**
     * Verifies that restore treats an all-absent checkpoint batch as already resolved instead of
     * replaying or failing it again.
     */
    @Test
    void testRestoreCommitSkipsAlreadyResolvedBatchWhenRecoveryScanHasNoCheckpointXid()
            throws Exception {
        JdbcSinkAggregatedCommitter committer = createCommitter();
        XaFacade xaFacade = mock(XaFacade.class);
        XaGroupOps xaGroupOps = mock(XaGroupOps.class);
        when(xaFacade.isOpen()).thenReturn(true);
        when(xaFacade.recover()).thenReturn(Collections.emptyList());
        setPrivateField(committer, "xaFacade", xaFacade);
        setPrivateField(committer, "xaGroupOps", xaGroupOps);
        JdbcAggregatedCommitInfo commitInfo =
                new JdbcAggregatedCommitInfo(
                        Arrays.asList(
                                new XidInfo(createXid(1, new byte[] {1}, new byte[] {2}), 0),
                                new XidInfo(createXid(2, new byte[] {3}, new byte[] {4}), 0)));

        Assertions.assertDoesNotThrow(
                () -> committer.restoreCommit(Collections.singletonList(commitInfo)));

        verify(xaGroupOps, never()).commit(anyList(), eq(false), eq(3));
    }

    /**
     * Verifies that restore reaches the still-prepared XID after an earlier XID was already
     * resolved by a previous failed attempt.
     */
    @Test
    void testRestoreCommitReportsRecoveredFailureBeforeAlreadyResolvedReplay() throws Exception {
        JdbcSinkAggregatedCommitter committer = createCommitter();
        XaFacade xaFacade = mock(XaFacade.class);
        XaGroupOps xaGroupOps = mock(XaGroupOps.class);
        RuntimeException stillPreparedFailure = new RuntimeException("still prepared failed");
        Xid alreadyResolvedXid = createXid(1, new byte[] {1}, new byte[] {1});
        Xid stillPreparedXid = createXid(2, new byte[] {2}, new byte[] {2});
        Xid recoveredStillPreparedXid = createXid(2, new byte[] {2}, new byte[] {2});
        when(xaFacade.isOpen()).thenReturn(true);
        when(xaFacade.recover()).thenReturn(Collections.singletonList(recoveredStillPreparedXid));
        when(xaGroupOps.commit(
                        argThat(
                                xids ->
                                        xids.size() == 1
                                                && xids.get(0).getXid() == stillPreparedXid),
                        eq(false),
                        eq(3)))
                .thenThrow(stillPreparedFailure);
        setPrivateField(committer, "xaFacade", xaFacade);
        setPrivateField(committer, "xaGroupOps", xaGroupOps);
        JdbcAggregatedCommitInfo commitInfo =
                new JdbcAggregatedCommitInfo(
                        Arrays.asList(
                                new XidInfo(alreadyResolvedXid, 0),
                                new XidInfo(stillPreparedXid, 0)));

        RuntimeException exception =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> committer.restoreCommit(Collections.singletonList(commitInfo)));

        Assertions.assertSame(stillPreparedFailure, exception);
        verify(xaGroupOps)
                .commit(
                        argThat(
                                xids ->
                                        xids.size() == 1
                                                && xids.get(0).getXid() == stillPreparedXid),
                        eq(false),
                        eq(3));
        verify(xaGroupOps, never())
                .commit(
                        argThat(
                                xids ->
                                        xids.stream()
                                                .anyMatch(
                                                        xidInfo ->
                                                                xidInfo.getXid()
                                                                        == alreadyResolvedXid)),
                        eq(false),
                        eq(3));
    }

    /**
     * Verifies that a missing XID after the first still-prepared transaction fails closed because
     * the batch order cannot explain it as an earlier successful commit.
     */
    @Test
    void testRestoreCommitFailsClosedOnMissingGapAfterRecoveredTransaction() throws Exception {
        JdbcSinkAggregatedCommitter committer = createCommitter();
        XaFacade xaFacade = mock(XaFacade.class);
        XaGroupOps xaGroupOps = mock(XaGroupOps.class);
        Xid stillPreparedXid = createXid(1, new byte[] {1}, new byte[] {1});
        Xid missingTailXid = createXid(2, new byte[] {2}, new byte[] {2});
        Xid recoveredStillPreparedXid = createXid(1, new byte[] {1}, new byte[] {1});
        when(xaFacade.isOpen()).thenReturn(true);
        when(xaFacade.recover()).thenReturn(Collections.singletonList(recoveredStillPreparedXid));
        setPrivateField(committer, "xaFacade", xaFacade);
        setPrivateField(committer, "xaGroupOps", xaGroupOps);
        JdbcAggregatedCommitInfo commitInfo =
                new JdbcAggregatedCommitInfo(
                        Arrays.asList(
                                new XidInfo(stillPreparedXid, 0), new XidInfo(missingTailXid, 0)));

        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () -> committer.restoreCommit(Collections.singletonList(commitInfo)));

        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains(
                                "is absent from the XA recovery scan after still-prepared transactions"));
        verify(xaGroupOps, never()).commit(anyList(), eq(false), eq(3));
    }

    /**
     * Verifies that restore retries a transient recovery-scan failure before replaying the
     * checkpoint transactions.
     */
    @Test
    void testRestoreCommitRetriesTransientRecoveryScanFailure() throws Exception {
        JdbcSinkAggregatedCommitter committer = createCommitter();
        XaFacade xaFacade = mock(XaFacade.class);
        XaGroupOps xaGroupOps = mock(XaGroupOps.class);
        XAException transientCause = new XAException(XAException.XAER_RMFAIL);
        Xid checkpointXid = createXid(1, new byte[] {1}, new byte[] {1});
        Xid recoveredXid = createXid(1, new byte[] {1}, new byte[] {1});
        when(xaFacade.isOpen()).thenReturn(true);
        when(xaFacade.recover())
                .thenThrow(new XaFacade.TransientXaException(transientCause))
                .thenReturn(Collections.singletonList(recoveredXid));
        when(xaGroupOps.commit(anyList(), eq(false), eq(3)))
                .thenReturn(new GroupXaOperationResult<>());
        setPrivateField(committer, "xaFacade", xaFacade);
        setPrivateField(committer, "xaGroupOps", xaGroupOps);
        JdbcAggregatedCommitInfo commitInfo =
                new JdbcAggregatedCommitInfo(
                        Collections.singletonList(new XidInfo(checkpointXid, 0)));

        Assertions.assertDoesNotThrow(
                () -> committer.restoreCommit(Collections.singletonList(commitInfo)));

        verify(xaFacade, times(2)).recover();
        verify(xaGroupOps).commit(anyList(), eq(false), eq(3));
    }

    /**
     * Verifies that Zeta retries are completed while the incremented attempt state is available.
     */
    @Test
    void testCommitCompletesBoundedRetriesWithinInvocation() throws Exception {
        JdbcConnectionConfig connectionConfig =
                JdbcConnectionConfig.builder().maxCommitAttempts(3).build();
        JdbcSinkConfig sinkConfig =
                JdbcSinkConfig.builder().jdbcConnectionConfig(connectionConfig).build();
        JdbcSinkAggregatedCommitter committer = new JdbcSinkAggregatedCommitter(sinkConfig);
        XaFacade xaFacade = mock(XaFacade.class);
        XaGroupOps xaGroupOps = mock(XaGroupOps.class);
        when(xaFacade.isOpen()).thenReturn(true);
        when(xaGroupOps.commit(anyList(), eq(false), eq(3)))
                .thenAnswer(
                        invocation -> {
                            XidInfo current = invocation.<List<XidInfo>>getArgument(0).get(0);
                            GroupXaOperationResult<XidInfo> result = new GroupXaOperationResult<>();
                            if (current.getAttempts() < 2) {
                                result.getForRetry().add(current.withAttemptsIncremented());
                            }
                            return result;
                        });
        setPrivateField(committer, "xaFacade", xaFacade);
        setPrivateField(committer, "xaGroupOps", xaGroupOps);
        JdbcAggregatedCommitInfo commitInfo =
                new JdbcAggregatedCommitInfo(
                        Collections.singletonList(new XidInfo(mock(Xid.class), 0)));

        committer.commit(Collections.singletonList(commitInfo));

        verify(xaGroupOps, times(3)).commit(anyList(), eq(false), eq(3));
    }

    /**
     * Verifies that the committer fails fast if an alternative XaGroupOps implementation never
     * drains or escalates the retry list.
     */
    @Test
    void testCommitFailsWhenRetryRoundsNeverDrain() throws Exception {
        JdbcSinkAggregatedCommitter committer = createCommitter();
        XaFacade xaFacade = mock(XaFacade.class);
        XaGroupOps xaGroupOps = mock(XaGroupOps.class);
        Xid xid = mock(Xid.class);
        when(xaFacade.isOpen()).thenReturn(true);
        when(xaGroupOps.commit(anyList(), eq(false), eq(3)))
                .thenAnswer(
                        invocation -> {
                            GroupXaOperationResult<XidInfo> result = new GroupXaOperationResult<>();
                            result.getForRetry().addAll(invocation.getArgument(0));
                            return result;
                        });
        setPrivateField(committer, "xaFacade", xaFacade);
        setPrivateField(committer, "xaGroupOps", xaGroupOps);
        JdbcAggregatedCommitInfo commitInfo =
                new JdbcAggregatedCommitInfo(Collections.singletonList(new XidInfo(xid, 0)));

        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () -> committer.commit(Collections.singletonList(commitInfo)));

        Assertions.assertTrue(exception.getMessage().contains("did not terminate"));
        verify(xaGroupOps, times(3)).commit(anyList(), eq(false), eq(3));
    }

    /**
     * Creates a committer with the same explicit retry limit used by every recovery test case.
     *
     * @return an uninitialized committer whose collaborators can be injected by the test
     */
    private JdbcSinkAggregatedCommitter createCommitter() {
        JdbcConnectionConfig connectionConfig =
                JdbcConnectionConfig.builder().maxCommitAttempts(3).build();
        JdbcSinkConfig sinkConfig =
                JdbcSinkConfig.builder().jdbcConnectionConfig(connectionConfig).build();
        return new JdbcSinkAggregatedCommitter(sinkConfig);
    }

    /**
     * Creates a driver-neutral XID value whose canonical components can be compared during
     * recovery.
     */
    private Xid createXid(int formatId, byte[] globalTransactionId, byte[] branchQualifier) {
        Xid xid = mock(Xid.class);
        when(xid.getFormatId()).thenReturn(formatId);
        when(xid.getGlobalTransactionId()).thenReturn(globalTransactionId);
        when(xid.getBranchQualifier()).thenReturn(branchQualifier);
        return xid;
    }

    /**
     * Injects a test dependency through the existing private field without changing production
     * constructors.
     */
    private void setPrivateField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}

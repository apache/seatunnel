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
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa.GroupXaOperationResult;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa.XaFacade;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa.XaGroupOps;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.transaction.xa.Xid;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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
     * Verifies that an XID absent from the recovery scan succeeds only when strict replay receives
     * an explicit successful resource-manager result.
     */
    @Test
    void testRestoreCommitAcceptsMissingTransactionAfterStrictSuccess() throws Exception {
        JdbcSinkAggregatedCommitter committer = createCommitter();
        XaFacade xaFacade = mock(XaFacade.class);
        XaGroupOps xaGroupOps = mock(XaGroupOps.class);
        when(xaFacade.isOpen()).thenReturn(true);
        when(xaFacade.recover()).thenReturn(Collections.emptyList());
        when(xaGroupOps.commit(anyList(), eq(false), eq(3)))
                .thenReturn(new GroupXaOperationResult<>());
        setPrivateField(committer, "xaFacade", xaFacade);
        setPrivateField(committer, "xaGroupOps", xaGroupOps);
        JdbcAggregatedCommitInfo commitInfo =
                new JdbcAggregatedCommitInfo(
                        Collections.singletonList(
                                new XidInfo(createXid(1, new byte[] {1}, new byte[] {2}), 0)));

        Assertions.assertDoesNotThrow(
                () -> committer.restoreCommit(Collections.singletonList(commitInfo)));

        verify(xaGroupOps).commit(anyList(), eq(false), eq(3));
    }

    /**
     * Verifies that an unknown restored XID propagates the strict commit failure instead of being
     * silently reported as committed.
     */
    @Test
    void testRestoreCommitPropagatesUnknownTransactionFailure() throws Exception {
        JdbcSinkAggregatedCommitter committer = createCommitter();
        XaFacade xaFacade = mock(XaFacade.class);
        XaGroupOps xaGroupOps = mock(XaGroupOps.class);
        RuntimeException unknownTransaction = new RuntimeException("unknown transaction");
        when(xaFacade.isOpen()).thenReturn(true);
        when(xaFacade.recover()).thenReturn(Collections.emptyList());
        when(xaGroupOps.commit(anyList(), eq(false), eq(3))).thenThrow(unknownTransaction);
        setPrivateField(committer, "xaFacade", xaFacade);
        setPrivateField(committer, "xaGroupOps", xaGroupOps);
        JdbcAggregatedCommitInfo commitInfo =
                new JdbcAggregatedCommitInfo(
                        Collections.singletonList(
                                new XidInfo(createXid(1, new byte[] {1}, new byte[] {2}), 0)));

        RuntimeException exception =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> committer.restoreCommit(Collections.singletonList(commitInfo)));

        Assertions.assertSame(unknownTransaction, exception);
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

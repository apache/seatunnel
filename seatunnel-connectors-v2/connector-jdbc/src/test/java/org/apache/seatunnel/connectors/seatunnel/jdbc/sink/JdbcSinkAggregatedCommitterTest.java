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

import org.junit.jupiter.api.Test;

import javax.transaction.xa.Xid;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests normal and recovery commit semantics for the JDBC aggregated committer. */
class JdbcSinkAggregatedCommitterTest {

    /**
     * Verifies that only restore commits can ignore an unknown transaction after a possibly lost
     * commit response.
     */
    @Test
    void testRestoreCommitEnablesIdempotentUnknownHandling() throws Exception {
        JdbcConnectionConfig connectionConfig =
                JdbcConnectionConfig.builder().maxCommitAttempts(3).build();
        JdbcSinkConfig sinkConfig =
                JdbcSinkConfig.builder().jdbcConnectionConfig(connectionConfig).build();
        JdbcSinkAggregatedCommitter committer = new JdbcSinkAggregatedCommitter(sinkConfig);
        XaFacade xaFacade = mock(XaFacade.class);
        XaGroupOps xaGroupOps = mock(XaGroupOps.class);
        when(xaFacade.isOpen()).thenReturn(true);
        when(xaGroupOps.commit(anyList(), eq(false), eq(false), eq(3)))
                .thenReturn(new GroupXaOperationResult<>());
        when(xaGroupOps.commit(anyList(), eq(false), eq(true), eq(3)))
                .thenReturn(new GroupXaOperationResult<>());
        setPrivateField(committer, "xaFacade", xaFacade);
        setPrivateField(committer, "xaGroupOps", xaGroupOps);
        JdbcAggregatedCommitInfo commitInfo =
                new JdbcAggregatedCommitInfo(
                        Collections.singletonList(new XidInfo(mock(Xid.class), 0)));

        committer.commit(Collections.singletonList(commitInfo));
        verify(xaGroupOps).commit(anyList(), eq(false), eq(false), eq(3));

        clearInvocations(xaGroupOps);
        committer.restoreCommit(Collections.singletonList(commitInfo));
        verify(xaGroupOps).commit(anyList(), eq(false), eq(true), eq(3));
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
        when(xaGroupOps.commit(anyList(), eq(false), eq(false), eq(3)))
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

        verify(xaGroupOps, times(3)).commit(anyList(), eq(false), eq(false), eq(3));
    }

    /** Injects a test dependency without changing the production constructor contract. */
    private void setPrivateField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}

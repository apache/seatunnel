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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa;

import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import java.util.ArrayList;
import java.util.Collections;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * Tests grouped XA commit failure propagation and retry accounting.
 *
 * <p>Each case verifies strict resource-manager commit handling for prepared transaction batches.
 */
class XaGroupOpsImplTest {

    /**
     * Verifies that a permanent commit failure propagates instead of being reported as a successful
     * checkpoint.
     */
    @Test
    void testCommitPropagatesPermanentFailure() {
        XaFacade xaFacade = mock(XaFacade.class);
        Xid xid = createXid();
        RuntimeException commitFailure = new RuntimeException("permanent commit failure");
        doThrow(commitFailure).when(xaFacade).commit(xid, false);
        XaGroupOps xaGroupOps = new XaGroupOpsImpl(xaFacade);

        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                xaGroupOps.commit(
                                        new ArrayList<>(
                                                Collections.singletonList(new XidInfo(xid, 0))),
                                        false,
                                        3));

        Assertions.assertSame(commitFailure, exception.getCause());
    }

    /**
     * Verifies that a transient failure is returned for retry with an incremented attempt count.
     */
    @Test
    void testCommitReturnsTransientFailureForRetry() {
        XaFacade xaFacade = mock(XaFacade.class);
        Xid xid = createXid();
        XAException transientCause = new XAException(XAException.XAER_RMFAIL);
        doThrow(new XaFacade.TransientXaException(transientCause))
                .when(xaFacade)
                .commit(xid, false);
        XaGroupOps xaGroupOps = new XaGroupOpsImpl(xaFacade);

        GroupXaOperationResult<XidInfo> result =
                xaGroupOps.commit(
                        new ArrayList<>(Collections.singletonList(new XidInfo(xid, 0))), false, 3);

        Assertions.assertEquals(1, result.getForRetry().size());
        Assertions.assertEquals(1, result.getForRetry().get(0).getAttempts());
        Assertions.assertSame(xid, result.getForRetry().get(0).getXid());
    }

    /**
     * Verifies that retry exhaustion fails the commit instead of silently discarding the
     * transaction.
     */
    @Test
    void testCommitFailsWhenTransientRetryLimitIsReached() {
        XaFacade xaFacade = mock(XaFacade.class);
        Xid xid = createXid();
        XAException transientCause = new XAException(XAException.XAER_RMFAIL);
        doThrow(new XaFacade.TransientXaException(transientCause))
                .when(xaFacade)
                .commit(xid, false);
        XaGroupOps xaGroupOps = new XaGroupOpsImpl(xaFacade);

        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                xaGroupOps.commit(
                                        new ArrayList<>(
                                                Collections.singletonList(new XidInfo(xid, 2))),
                                        false,
                                        3));

        Assertions.assertTrue(exception.getMessage().contains("reached max number"));
    }

    /**
     * Creates a stable transaction identifier whose value is shared across grouped commit tests.
     */
    private Xid createXid() {
        return new XidImpl(1, new byte[] {1}, new byte[] {1});
    }
}

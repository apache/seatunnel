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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import java.lang.reflect.Field;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests XA error classification and explicit recovery outcomes without requiring a database
 * resource manager.
 */
class XaFacadeImplAutoLoadTest {

    /**
     * Verifies that resource-manager availability errors remain retryable instead of being wrapped
     * as permanent connector failures.
     */
    @Test
    void testCommitPreservesTransientXaException() throws Exception {
        XAResource xaResource = mock(XAResource.class);
        XAException transientFailure = new XAException(XAException.XAER_RMFAIL);
        doThrow(transientFailure).when(xaResource).commit(any(), eq(false));
        XaFacadeImplAutoLoad xaFacade = createOpenFacade(xaResource);

        XaFacade.TransientXaException exception =
                Assertions.assertThrows(
                        XaFacade.TransientXaException.class,
                        () -> xaFacade.commit(createXid(), false));

        Assertions.assertSame(transientFailure, exception.getCause());
    }

    /**
     * Verifies that XA_RETRY remains retryable because the resource manager reports that the
     * operation had no effect.
     */
    @Test
    void testCommitRetriesWhenResourceManagerRequestsRetry() throws Exception {
        XAResource xaResource = mock(XAResource.class);
        XAException retry = new XAException(XAException.XA_RETRY);
        doThrow(retry).when(xaResource).commit(any(), eq(false));
        XaFacadeImplAutoLoad xaFacade = createOpenFacade(xaResource);

        XaFacade.TransientXaException exception =
                Assertions.assertThrows(
                        XaFacade.TransientXaException.class,
                        () -> xaFacade.commit(createXid(), false));

        Assertions.assertSame(retry, exception.getCause());
    }

    /**
     * Verifies that an explicit heuristic-commit result is accepted as durable completion evidence.
     */
    @Test
    void testCommitAcceptsHeuristicCommitResult() throws Exception {
        XAResource xaResource = mock(XAResource.class);
        XAException heuristicallyCommitted = new XAException(XAException.XA_HEURCOM);
        doThrow(heuristicallyCommitted).when(xaResource).commit(any(), eq(false));
        XaFacadeImplAutoLoad xaFacade = createOpenFacade(xaResource);

        Assertions.assertDoesNotThrow(() -> xaFacade.commit(createXid(), false));

        verify(xaResource).forget(any());
    }

    /**
     * Verifies that a transaction reported as rolled back is a permanent failure and is never
     * retried as though its commit outcome were unknown.
     */
    @Test
    void testCommitDoesNotRetryRolledBackTransaction() throws Exception {
        XAResource xaResource = mock(XAResource.class);
        XAException rolledBack = new XAException(XAException.XA_RBTRANSIENT);
        doThrow(rolledBack).when(xaResource).commit(any(), eq(false));
        XaFacadeImplAutoLoad xaFacade = createOpenFacade(xaResource);

        Assertions.assertThrows(
                JdbcConnectorException.class, () -> xaFacade.commit(createXid(), false));
    }

    /**
     * Verifies that XAER_NOTA is ignored only when recovery is repeating a commit whose response
     * may have been lost.
     */
    @Test
    void testCommitIgnoresUnknownTransactionOnlyWhenRequested() throws Exception {
        XAResource xaResource = mock(XAResource.class);
        XAException unknownTransaction = new XAException(XAException.XAER_NOTA);
        doThrow(unknownTransaction).when(xaResource).commit(any(), eq(false));
        XaFacadeImplAutoLoad xaFacade = createOpenFacade(xaResource);
        Xid xid = createXid();

        Assertions.assertDoesNotThrow(() -> xaFacade.commit(xid, true));
        Assertions.assertThrows(JdbcConnectorException.class, () -> xaFacade.commit(xid, false));
    }

    /**
     * Creates a facade with an injected XA resource so error handling can be tested without a
     * database.
     */
    private XaFacadeImplAutoLoad createOpenFacade(XAResource xaResource) throws Exception {
        XaFacadeImplAutoLoad xaFacade = new XaFacadeImplAutoLoad(null);
        Field field = XaFacadeImplAutoLoad.class.getDeclaredField("xaResource");
        field.setAccessible(true);
        field.set(xaFacade, xaResource);
        return xaFacade;
    }

    /**
     * Creates a stable transaction identifier whose value is shared across the commit test cases.
     */
    private Xid createXid() {
        return new XidImpl(1, new byte[] {1}, new byte[] {1});
    }
}

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
import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa.XaFacade;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa.XaGroupOps;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa.XidGenerator;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import javax.transaction.xa.Xid;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JdbcExactlyOnceSinkWriterTest {

    @Test
    void testPrepareCommitWithSameCheckpointGeneratesMonotonicTxIds() throws Exception {
        TestContext context = createWriter();

        context.writer.prepareCommit(100L);
        context.writer.prepareCommit(100L);

        ArgumentCaptor<Long> txIdCaptor = ArgumentCaptor.forClass(Long.class);
        verify(context.xidGenerator, times(3)).generateXid(any(), any(), txIdCaptor.capture());
        List<Long> txIds = txIdCaptor.getAllValues();
        Assertions.assertEquals(3, txIds.size());
        Assertions.assertTrue(txIds.get(1) > txIds.get(0));
        Assertions.assertTrue(txIds.get(2) > txIds.get(1));
    }

    @Test
    void testPrepareCommitRollbackPreparedXidWhenStartNextTxFailed() throws Exception {
        TestContext context = createWriter();

        doNothing()
                .doThrow(new RuntimeException("start next tx failed"))
                .when(context.xaFacade)
                .start(any());

        Assertions.assertThrows(
                JdbcConnectorException.class, () -> context.writer.prepareCommit(10L));

        ArgumentCaptor<Xid> startXidCaptor = ArgumentCaptor.forClass(Xid.class);
        verify(context.xaFacade, times(2)).start(startXidCaptor.capture());
        Xid preparedXid = startXidCaptor.getAllValues().get(0);
        verify(context.xaFacade, times(1)).rollback(preparedXid);
    }

    @Test
    void testPrepareCommitThrowWhenRollbackPreparedXidFailedAfterBeginNextTxFailed()
            throws Exception {
        TestContext context = createWriter();

        doNothing()
                .doThrow(new RuntimeException("start next tx failed"))
                .when(context.xaFacade)
                .start(any());
        doThrow(new RuntimeException("rollback prepared failed"))
                .when(context.xaFacade)
                .rollback(any());

        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class, () -> context.writer.prepareCommit(10L));

        Assertions.assertTrue(exception.getMessage().contains("rollback prepared transaction"));
        Assertions.assertEquals(1, exception.getSuppressed().length);
        Assertions.assertTrue(
                exception
                        .getSuppressed()[0]
                        .getMessage()
                        .contains("unable to start xa transaction"));
        ArgumentCaptor<Xid> recoverExcludeXidCaptor = ArgumentCaptor.forClass(Xid.class);
        verify(context.xaGroupOps, times(1))
                .recoverAndRollback(any(), any(), any(), recoverExcludeXidCaptor.capture());
        Assertions.assertNull(recoverExcludeXidCaptor.getValue());
    }

    @Test
    void testPrepareCommitAttachRecoveryFailureWhenRollbackAndRecoveryBothFailed()
            throws Exception {
        TestContext context = createWriter();

        doNothing()
                .doThrow(new RuntimeException("start next tx failed"))
                .when(context.xaFacade)
                .start(any());
        doThrow(new RuntimeException("rollback prepared failed"))
                .when(context.xaFacade)
                .rollback(any());
        doThrow(new RuntimeException("recover failed"))
                .when(context.xaGroupOps)
                .recoverAndRollback(any(), any(), any(), any());

        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class, () -> context.writer.prepareCommit(10L));

        Assertions.assertTrue(exception.getMessage().contains("rollback prepared transaction"));
        Assertions.assertEquals(2, exception.getSuppressed().length);
        Assertions.assertTrue(
                exception
                        .getSuppressed()[0]
                        .getMessage()
                        .contains("unable to start xa transaction"));
        Assertions.assertTrue(exception.getSuppressed()[1].getMessage().contains("recover failed"));
    }

    @Test
    void testPrepareCommitWithEmptyTransactionDontRollbackPreparedXidWhenStartNextTxFailed()
            throws Exception {
        TestContext context = createWriter();

        doThrow(mock(XaFacade.EmptyXaTransactionException.class))
                .when(context.xaFacade)
                .endAndPrepare(any());
        doNothing()
                .doThrow(new RuntimeException("start next tx failed"))
                .when(context.xaFacade)
                .start(any());

        Assertions.assertThrows(
                JdbcConnectorException.class, () -> context.writer.prepareCommit(10L));

        verify(context.xaFacade, never()).rollback(any());
        Assertions.assertNull(getPrivateField(context.writer, "prepareXid"));
    }

    @Test
    void testInjectedConstructorOpenXidGeneratorOnFirstUse() throws Exception {
        TestContext context = createWriter();

        verify(context.xidGenerator, never()).open();

        context.writer.prepareCommit(10L);

        verify(context.xidGenerator, times(1)).open();
    }

    @Test
    void testTryOpenSkipRecoverAndRollbackWhenRecoverStateIsEmpty() throws Exception {
        TestContext context = createWriter();

        context.writer.prepareCommit(10L);

        verify(context.xaGroupOps, never()).recoverAndRollback(any(), any(), any(), any());
    }

    @Test
    void testTryOpenRecoverAndRollbackWhenRecoverStatePresent() throws Exception {
        Xid recoveredStateXid = new TestXid(10L);
        TestContext context =
                createWriter(Collections.singletonList(new JdbcSinkState(recoveredStateXid)));

        context.writer.prepareCommit(10L);

        ArgumentCaptor<Xid> excludeXidCaptor = ArgumentCaptor.forClass(Xid.class);
        verify(context.xaGroupOps, times(1))
                .recoverAndRollback(any(), any(), any(), excludeXidCaptor.capture());
        Assertions.assertSame(recoveredStateXid, excludeXidCaptor.getValue());
    }

    @Test
    void testAbortPrepareRollbackPreparedAndCurrentTransaction() throws Exception {
        TestContext context = createWriter();

        Xid preparedXid = new TestXid(1L);
        Xid currentXid = new TestXid(2L);
        setPrivateField(context.writer, "prepareXid", preparedXid);
        setPrivateField(context.writer, "currentXid", currentXid);

        context.writer.abortPrepare();
        verify(context.xaFacade, times(1)).rollback(preparedXid);
        verify(context.xaFacade, times(1)).failAndRollback(currentXid);
        Assertions.assertNull(getPrivateField(context.writer, "prepareXid"));
        Assertions.assertNull(getPrivateField(context.writer, "currentXid"));

        clearInvocations(context.xaFacade);
        context.writer.abortPrepare();
        verify(context.xaFacade, never()).rollback(any());
        verify(context.xaFacade, never()).failAndRollback(any());
    }

    @Test
    void testCloseRollbackCurrentTransactionOnly() throws Exception {
        TestContext context = createWriter();

        Xid preparedXid = new TestXid(3L);
        Xid currentXid = new TestXid(4L);
        setPrivateField(context.writer, "prepareXid", preparedXid);
        setPrivateField(context.writer, "currentXid", currentXid);

        context.writer.close();

        verify(context.xaFacade, never()).rollback(any());
        verify(context.xaFacade, times(1)).failAndRollback(currentXid);
        verify(context.xaFacade, times(1)).close();
        verify(context.outputFormat, times(1)).close();
        verify(context.xidGenerator, times(1)).close();
        Assertions.assertNull(getPrivateField(context.writer, "prepareXid"));
        Assertions.assertNull(getPrivateField(context.writer, "currentXid"));
    }

    private TestContext createWriter() throws Exception {
        return createWriter(Collections.<JdbcSinkState>emptyList());
    }

    private TestContext createWriter(List<JdbcSinkState> states) throws Exception {
        SinkWriter.Context sinkWriterContext = new DefaultSinkWriterContext(0, 1);
        JobContext jobContext = new JobContext(1L);
        XaFacade xaFacade = mock(XaFacade.class);
        XaGroupOps xaGroupOps = mock(XaGroupOps.class);
        XidGenerator xidGenerator = mock(XidGenerator.class);
        JdbcOutputFormat<SeaTunnelRow, JdbcBatchStatementExecutor<SeaTunnelRow>> outputFormat =
                mock(JdbcOutputFormat.class);

        when(xaFacade.isOpen()).thenReturn(true);
        when(xidGenerator.generateXid(any(), any(), anyLong()))
                .thenAnswer(invocation -> new TestXid((Long) invocation.getArguments()[2]));

        JdbcExactlyOnceSinkWriter writer =
                new JdbcExactlyOnceSinkWriter(
                        sinkWriterContext,
                        jobContext,
                        states,
                        xaFacade,
                        xaGroupOps,
                        xidGenerator,
                        outputFormat);
        return new TestContext(writer, xaFacade, xaGroupOps, xidGenerator, outputFormat);
    }

    private static void setPrivateField(Object target, String fieldName, Object value)
            throws Exception {
        Field field = JdbcExactlyOnceSinkWriter.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object getPrivateField(Object target, String fieldName) throws Exception {
        Field field = JdbcExactlyOnceSinkWriter.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static class TestContext {
        private final JdbcExactlyOnceSinkWriter writer;
        private final XaFacade xaFacade;
        private final XaGroupOps xaGroupOps;
        private final XidGenerator xidGenerator;
        private final JdbcOutputFormat<SeaTunnelRow, JdbcBatchStatementExecutor<SeaTunnelRow>>
                outputFormat;

        private TestContext(
                JdbcExactlyOnceSinkWriter writer,
                XaFacade xaFacade,
                XaGroupOps xaGroupOps,
                XidGenerator xidGenerator,
                JdbcOutputFormat<SeaTunnelRow, JdbcBatchStatementExecutor<SeaTunnelRow>>
                        outputFormat) {
            this.writer = writer;
            this.xaFacade = xaFacade;
            this.xaGroupOps = xaGroupOps;
            this.xidGenerator = xidGenerator;
            this.outputFormat = outputFormat;
        }
    }

    private static class TestXid implements Xid {
        private final long txId;

        private TestXid(long txId) {
            this.txId = txId;
        }

        @Override
        public int getFormatId() {
            return 201;
        }

        @Override
        public byte[] getGlobalTransactionId() {
            return new byte[] {
                (byte) txId, (byte) (txId >>> 8), (byte) (txId >>> 16), (byte) (txId >>> 24)
            };
        }

        @Override
        public byte[] getBranchQualifier() {
            return new byte[] {0, 0, 0, 1};
        }
    }
}

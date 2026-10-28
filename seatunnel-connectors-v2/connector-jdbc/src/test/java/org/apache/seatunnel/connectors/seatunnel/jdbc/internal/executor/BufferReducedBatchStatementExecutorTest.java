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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class BufferReducedBatchStatementExecutorTest extends BufferExecutorTest {
    @Override
    JdbcBatchStatementExecutor<SeaTunnelRow> getExecutorWithBatchRecorder(
            List<SeaTunnelRow> recorder) {
        return new BufferReducedBatchStatementExecutor(
                new JdbcBatchStatementExecutor<SeaTunnelRow>() {
                    @Override
                    public void prepareStatements(Connection connection) throws SQLException {}

                    @Override
                    public void addToBatch(SeaTunnelRow record) throws SQLException {
                        recorder.add(record);
                    }

                    @Override
                    public void executeBatch() throws SQLException {
                        throw new SQLException("test");
                    }

                    @Override
                    public void closeStatements() throws SQLException {}
                },
                new JdbcBatchStatementExecutor<SeaTunnelRow>() {
                    @Override
                    public void prepareStatements(Connection connection) throws SQLException {}

                    @Override
                    public void addToBatch(SeaTunnelRow record) throws SQLException {
                        recorder.add(record);
                    }

                    @Override
                    public void executeBatch() throws SQLException {
                        throw new SQLException("test");
                    }

                    @Override
                    public void closeStatements() throws SQLException {}
                },
                Function.identity(),
                Function.identity());
    }

    @Test
    void testReducedRowKeepsTheNetEffectOfTheBatch() throws SQLException {
        List<SeaTunnelRow> upserts = new ArrayList<>();
        List<SeaTunnelRow> deletes = new ArrayList<>();
        JdbcBatchStatementExecutor<SeaTunnelRow> executor = newKeyedExecutor(upserts, deletes);
        // Created then updated in one batch: still has to be inserted.
        executor.addToBatch(row(RowKind.INSERT, 1, 1, "a"));
        executor.addToBatch(row(RowKind.UPDATE_BEFORE, 1, 1, "a"));
        executor.addToBatch(row(RowKind.UPDATE_AFTER, 1, 1, "b"));
        // Existing row updated twice.
        executor.addToBatch(row(RowKind.UPDATE_AFTER, 2, 2, "x"));
        executor.addToBatch(row(RowKind.UPDATE_AFTER, 2, 2, "y"));
        // Existing row deleted and re-inserted: the row is still there, so update it.
        executor.addToBatch(row(RowKind.DELETE, 3, 3, "p"));
        executor.addToBatch(row(RowKind.INSERT, 3, 3, "q"));
        // Existing row updated then deleted.
        executor.addToBatch(row(RowKind.UPDATE_AFTER, 4, 4, "m"));
        executor.addToBatch(row(RowKind.DELETE, 4, 4, "m"));
        executor.executeBatch();

        Assertions.assertEquals(
                Arrays.asList("+I[1, 1, b]", "+U[2, 2, y]", "+U[3, 3, q]"), describe(upserts));
        Assertions.assertEquals(Collections.singletonList("+I[4]"), describe(deletes));

        // A new batch judges existence afresh.
        upserts.clear();
        executor.addToBatch(row(RowKind.UPDATE_AFTER, 1, 1, "c"));
        executor.executeBatch();
        Assertions.assertEquals(Collections.singletonList("+U[1, 1, c]"), describe(upserts));
    }

    private static List<String> describe(List<SeaTunnelRow> rows) {
        List<String> described = new ArrayList<>();
        for (SeaTunnelRow row : rows) {
            described.add(row.getRowKind().shortString() + Arrays.toString(row.getFields()));
        }
        return described;
    }

    @Test
    void testRowsWithSameKeyAreReduced() throws SQLException {
        List<SeaTunnelRow> upserts = new ArrayList<>();
        JdbcBatchStatementExecutor<SeaTunnelRow> executor =
                newKeyedExecutor(upserts, new ArrayList<>());

        executor.addToBatch(row(RowKind.INSERT, 1, 1, "a"));
        executor.addToBatch(row(RowKind.UPDATE_AFTER, 2, 1, "b"));
        executor.executeBatch();

        Assertions.assertEquals(1, upserts.size());
        Assertions.assertEquals("b", upserts.get(0).getField(2));
    }

    @Test
    void testRowsWithNullKeyAreNotReduced() throws SQLException {
        List<SeaTunnelRow> upserts = new ArrayList<>();
        JdbcBatchStatementExecutor<SeaTunnelRow> executor =
                newKeyedExecutor(upserts, new ArrayList<>());

        executor.addToBatch(row(RowKind.INSERT, 1, null, "a"));
        executor.addToBatch(row(RowKind.INSERT, 2, null, "b"));
        executor.addToBatch(row(RowKind.INSERT, 3, 7, "c"));
        executor.executeBatch();

        // NULL never equals NULL in SQL, so the database keeps both rows with a NULL key
        Assertions.assertEquals(3, upserts.size());
        Assertions.assertEquals(
                Arrays.asList(1, 2, 3),
                Arrays.asList(
                        upserts.get(0).getField(0),
                        upserts.get(1).getField(0),
                        upserts.get(2).getField(0)));
    }

    @Test
    void testDeleteWithNullKeyKeepsOrderWithUpserts() throws SQLException {
        List<SeaTunnelRow> upserts = new ArrayList<>();
        List<SeaTunnelRow> deletes = new ArrayList<>();
        JdbcBatchStatementExecutor<SeaTunnelRow> executor = newKeyedExecutor(upserts, deletes);

        executor.addToBatch(row(RowKind.INSERT, 1, null, "a"));
        executor.addToBatch(row(RowKind.DELETE, 1, null, "a"));
        executor.executeBatch();

        Assertions.assertEquals(1, upserts.size());
        Assertions.assertEquals(1, deletes.size());
        // The delete executor receives the extracted key, not the whole row
        Assertions.assertEquals(1, deletes.get(0).getArity());
    }

    /** Executor keyed by the second field (a nullable unique key column). */
    private static JdbcBatchStatementExecutor<SeaTunnelRow> newKeyedExecutor(
            List<SeaTunnelRow> upserts, List<SeaTunnelRow> deletes) {
        return new BufferReducedBatchStatementExecutor(
                new RecordingExecutor(upserts),
                new RecordingExecutor(deletes),
                row -> new SeaTunnelRow(new Object[] {row.getField(1)}),
                Function.identity());
    }

    private static SeaTunnelRow row(RowKind rowKind, Integer id, Integer code, String name) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, code, name});
        row.setRowKind(rowKind);
        return row;
    }

    private static class RecordingExecutor implements JdbcBatchStatementExecutor<SeaTunnelRow> {
        private final List<SeaTunnelRow> recorder;

        RecordingExecutor(List<SeaTunnelRow> recorder) {
            this.recorder = recorder;
        }

        @Override
        public void prepareStatements(Connection connection) {}

        @Override
        public void addToBatch(SeaTunnelRow record) {
            recorder.add(record);
        }

        @Override
        public void executeBatch() {}

        @Override
        public void closeStatements() {}
    }

    private final JdbcBatchStatementExecutor<SeaTunnelRow> upsert =
            mock(JdbcBatchStatementExecutor.class);
    private final JdbcBatchStatementExecutor<SeaTunnelRow> delete =
            mock(JdbcBatchStatementExecutor.class);
    private final BufferReducedBatchStatementExecutor executor =
            new BufferReducedBatchStatementExecutor(
                    upsert,
                    delete,
                    row -> new SeaTunnelRow(new Object[] {row.getField(0)}),
                    Function.identity());

    private SeaTunnelRow row(int id, RowKind kind) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id});
        row.setRowKind(kind);
        return row;
    }

    @Test
    void insertUpdateAndCloseDoNotPrepareDeleteEvenWhenPermissionIsDenied() throws SQLException {
        Connection connection = mock(Connection.class);
        doThrow(new SQLException("insufficient DELETE privilege", "42000", 258))
                .when(delete)
                .prepareStatements(any());
        executor.prepareStatements(connection);
        executor.addToBatch(row(1, RowKind.INSERT));
        executor.addToBatch(row(2, RowKind.UPDATE_AFTER));
        executor.addToBatch(row(3, RowKind.UPDATE_BEFORE));
        executor.executeBatch();
        executor.closeStatements();
        verify(upsert).prepareStatements(connection);
        verify(upsert, times(2)).addToBatch(any());
        verify(delete, never()).prepareStatements(any());
        verify(delete, never()).addToBatch(any());
        verify(delete, never()).executeBatch();
    }

    @Test
    void realDeletesPrepareOnceAndRetainDeletion() throws SQLException {
        Connection connection = mock(Connection.class);
        executor.prepareStatements(connection);
        executor.addToBatch(row(1, RowKind.DELETE));
        executor.executeBatch();
        executor.addToBatch(row(2, RowKind.DELETE));
        executor.executeBatch();
        verify(delete).prepareStatements(connection);
        verify(delete, times(2)).addToBatch(any());
        verify(delete, times(2)).executeBatch();
    }

    @Test
    void reducedDeleteOverwrittenByInsertDoesNotRequestDeletePermission() throws SQLException {
        executor.prepareStatements(mock(Connection.class));
        executor.addToBatch(row(1, RowKind.DELETE));
        executor.addToBatch(row(1, RowKind.INSERT));
        executor.executeBatch();
        verify(delete, never()).prepareStatements(any());
        verify(upsert).addToBatch(any());
    }

    @Test
    void deniedRealDeletePropagatesAndIsPreparedAgainOnRetry() throws SQLException {
        Connection connection = mock(Connection.class);
        doThrow(new SQLException("insufficient privilege", "42000", 258))
                .doNothing()
                .when(delete)
                .prepareStatements(connection);
        executor.prepareStatements(connection);
        executor.addToBatch(row(1, RowKind.DELETE));
        assertThrows(SQLException.class, executor::executeBatch);
        verify(delete, never()).addToBatch(any());
        executor.executeBatch();
        verify(delete, times(2)).prepareStatements(connection);
        verify(delete).addToBatch(any());
        verify(delete).executeBatch();
    }

    @Test
    void reconnectUsesTheNewConnectionAndPreparesDeleteLazilyAgain() throws SQLException {
        Connection first = mock(Connection.class);
        Connection second = mock(Connection.class);
        executor.prepareStatements(first);
        executor.addToBatch(row(1, RowKind.DELETE));
        executor.executeBatch();
        executor.closeStatements();
        executor.prepareStatements(second);
        executor.addToBatch(row(2, RowKind.INSERT));
        executor.executeBatch();
        verify(delete, never()).prepareStatements(second);
        executor.addToBatch(row(3, RowKind.DELETE));
        executor.executeBatch();
        executor.closeStatements();
        verify(delete).prepareStatements(first);
        verify(delete).prepareStatements(second);
        verify(delete, times(2)).closeStatements();
    }

    @Test
    void mixedChangesKeepExistingBatchOrder() throws SQLException {
        Connection connection = mock(Connection.class);
        executor.prepareStatements(connection);
        executor.addToBatch(row(1, RowKind.INSERT));
        executor.addToBatch(row(2, RowKind.DELETE));
        executor.addToBatch(row(3, RowKind.UPDATE_AFTER));
        executor.executeBatch();
        org.mockito.InOrder order = inOrder(upsert, delete);
        order.verify(upsert).prepareStatements(connection);
        order.verify(upsert).addToBatch(any());
        order.verify(upsert).executeBatch();
        order.verify(delete).prepareStatements(connection);
        order.verify(delete).addToBatch(any());
        order.verify(delete).executeBatch();
        order.verify(upsert).addToBatch(any());
        order.verify(upsert).executeBatch();
    }

    @Test
    void reconnectWithoutDeleteDoesNotClearOrCloseAnOldStatement() throws SQLException {
        Connection first = mock(Connection.class);
        Connection second = mock(Connection.class);
        PreparedStatement oldStatement = mock(PreparedStatement.class);
        PreparedStatement newStatement = mock(PreparedStatement.class);
        AtomicBoolean oldClosed = new AtomicBoolean();
        org.mockito.Mockito.doAnswer(
                        call -> {
                            if (oldClosed.get()) {
                                throw new SQLException("statement is closed");
                            }
                            return null;
                        })
                .when(oldStatement)
                .clearBatch();
        org.mockito.Mockito.doAnswer(
                        call -> {
                            oldClosed.set(true);
                            return null;
                        })
                .when(oldStatement)
                .close();
        JdbcBatchStatementExecutor<SeaTunnelRow> upserts = mock(JdbcBatchStatementExecutor.class);
        SimpleBatchStatementExecutor deletes =
                new SimpleBatchStatementExecutor(
                        connection -> connection == first ? oldStatement : newStatement,
                        TableSchema.builder().build(),
                        null,
                        mock(JdbcRowConverter.class));
        BufferReducedBatchStatementExecutor reduced =
                new BufferReducedBatchStatementExecutor(
                        upserts, deletes, Function.identity(), Function.identity());
        reduced.prepareStatements(first);
        reduced.addToBatch(row(1, RowKind.DELETE));
        reduced.executeBatch();
        reduced.closeStatements();
        reduced.prepareStatements(second);
        doThrow(new SQLException("row error", "22001")).when(upserts).executeBatch();
        reduced.addToBatch(row(2, RowKind.INSERT));
        assertThrows(SQLException.class, reduced::executeBatch);
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(reduced::clearBatch);
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(reduced::closeStatements);
        verify(oldStatement).close();
        org.mockito.Mockito.verifyNoInteractions(newStatement);
    }
}

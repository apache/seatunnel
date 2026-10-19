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

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

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

    @Test
    void testCompareKeysIsTransitiveAcrossDifferentTypes() {
        SeaTunnelRow integerTwo = new SeaTunnelRow(new Object[] {2});
        SeaTunnelRow integerTen = new SeaTunnelRow(new Object[] {10});
        SeaTunnelRow stringEleven = new SeaTunnelRow(new Object[] {"11"});

        int twoToTen = BufferReducedBatchStatementExecutor.compareKeys(integerTwo, integerTen);
        int tenToString = BufferReducedBatchStatementExecutor.compareKeys(integerTen, stringEleven);
        int twoToString = BufferReducedBatchStatementExecutor.compareKeys(integerTwo, stringEleven);

        Assertions.assertTrue(twoToTen < 0);
        Assertions.assertTrue(tenToString < 0);
        Assertions.assertTrue(twoToString < 0);
    }

    @Test
    void testExecuteBatchReplaysKeysInDeterministicOrder() throws SQLException {
        List<String> recorder = new ArrayList<>();
        BufferReducedBatchStatementExecutor executor =
                new BufferReducedBatchStatementExecutor(
                        new TaggedRecordingExecutor(recorder, "U"),
                        new TaggedRecordingExecutor(recorder, "D"),
                        row -> new SeaTunnelRow(new Object[] {row.getField(0)}),
                        Function.identity(),
                        true);

        executor.addToBatch(row(10, "ten"));
        executor.addToBatch(row(2, "two"));
        executor.addToBatch(row(11, "eleven"));
        executor.executeBatch();

        Assertions.assertEquals(Arrays.asList("U:2", "U:10", "U:11", "U:flush"), recorder);
    }

    @Test
    void testTransactionConflictResetRetainsLogicalBuffer() throws SQLException {
        List<String> recorder = new ArrayList<>();
        RetryRecordingExecutor upsert = new RetryRecordingExecutor(recorder, "U");
        BufferReducedBatchStatementExecutor executor =
                new BufferReducedBatchStatementExecutor(
                        upsert,
                        new TaggedRecordingExecutor(recorder, "D"),
                        Function.identity(),
                        Function.identity());

        executor.addToBatch(row(1, "one"));
        Assertions.assertThrows(SQLException.class, executor::executeBatch);
        executor.resetForTransactionConflictRetry();
        executor.executeBatch();

        Assertions.assertEquals(Arrays.asList("U:1", "U:1", "U:flush"), recorder);
        Assertions.assertEquals(1, upsert.clearBatchCalls);
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

    private static SeaTunnelRow row(Object key, String value) {
        return new SeaTunnelRow(new Object[] {key, value});
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

    private static class TaggedRecordingExecutor
            implements JdbcBatchStatementExecutor<SeaTunnelRow> {
        protected final List<String> recorder;
        protected final String tag;

        private TaggedRecordingExecutor(List<String> recorder, String tag) {
            this.recorder = recorder;
            this.tag = tag;
        }

        @Override
        public void prepareStatements(Connection connection) {}

        @Override
        public void addToBatch(SeaTunnelRow record) {
            recorder.add(tag + ":" + record.getField(0));
        }

        @Override
        public void executeBatch() {
            recorder.add(tag + ":flush");
        }

        @Override
        public void closeStatements() {}
    }

    private static final class RetryRecordingExecutor extends TaggedRecordingExecutor {
        private int executeBatchCalls;
        private int clearBatchCalls;

        private RetryRecordingExecutor(List<String> recorder, String tag) {
            super(recorder, tag);
        }

        @Override
        public void executeBatch() throws SQLException {
            executeBatchCalls++;
            if (executeBatchCalls == 1) {
                throw new SQLException("deadlock", "40P01");
            }
            super.executeBatch();
        }

        @Override
        public void clearBatch() {
            clearBatchCalls++;
        }
    }
}

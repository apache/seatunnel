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
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class OrderedBufferedBatchStatementExecutorTest extends BufferExecutorTest {

    @Override
    JdbcBatchStatementExecutor<SeaTunnelRow> getExecutorWithBatchRecorder(
            List<SeaTunnelRow> recorder) {
        return new OrderedBufferedBatchStatementExecutor(
                new FailingExecutor(recorder),
                new FailingExecutor(recorder),
                Function.identity(),
                Function.identity());
    }

    @Test
    void testSameKeyChangesKeepArrivalOrderInEitherOrder() throws SQLException {
        List<String> log = new ArrayList<>();
        JdbcBatchStatementExecutor<SeaTunnelRow> executor = newExecutor(log);

        // newer -> older
        executor.addToBatch(row(RowKind.INSERT, 1, "v300", 300));
        executor.addToBatch(row(RowKind.INSERT, 1, "v200", 200));
        // older -> newer
        executor.addToBatch(row(RowKind.UPDATE_AFTER, 2, "v200", 200));
        executor.addToBatch(row(RowKind.UPDATE_AFTER, 2, "v300", 300));
        executor.executeBatch();

        Assertions.assertEquals(
                Arrays.asList(
                        "upsert[1, v300, 300]",
                        "execute upsert",
                        "upsert[1, v200, 200]",
                        "upsert[2, v200, 200]",
                        "execute upsert",
                        "upsert[2, v300, 300]",
                        "execute upsert"),
                log);
    }

    @Test
    void testDistinctKeysShareOneStatementBatch() throws SQLException {
        List<String> log = new ArrayList<>();
        JdbcBatchStatementExecutor<SeaTunnelRow> executor = newExecutor(log);

        executor.addToBatch(row(RowKind.INSERT, 1, "a", 1));
        executor.addToBatch(row(RowKind.INSERT, 2, "b", 1));
        executor.addToBatch(row(RowKind.INSERT, 3, "c", 1));
        executor.executeBatch();

        Assertions.assertEquals(
                Arrays.asList(
                        "upsert[1, a, 1]", "upsert[2, b, 1]", "upsert[3, c, 1]", "execute upsert"),
                log);
    }

    @Test
    void testNullKeyNeverStartsANewStatementBatch() throws SQLException {
        List<String> log = new ArrayList<>();
        JdbcBatchStatementExecutor<SeaTunnelRow> executor = newExecutor(log);

        executor.addToBatch(row(RowKind.INSERT, null, "a", 1));
        executor.addToBatch(row(RowKind.INSERT, null, "b", 1));
        executor.executeBatch();

        // NULL never equals NULL in SQL, so these rows can not change the same stored row.
        Assertions.assertEquals(
                Arrays.asList("upsert[null, a, 1]", "upsert[null, b, 1]", "execute upsert"), log);
    }

    @Test
    void testCompositeKeyStartsANewBatchOnlyForTheSameWholeKey() throws SQLException {
        List<String> log = new ArrayList<>();
        JdbcBatchStatementExecutor<SeaTunnelRow> executor =
                new OrderedBufferedBatchStatementExecutor(
                        new LoggingExecutor("upsert", log),
                        new LoggingExecutor("delete", log),
                        row -> new SeaTunnelRow(new Object[] {row.getField(0), row.getField(1)}),
                        Function.identity());

        executor.addToBatch(row(RowKind.INSERT, 1, "a", 1));
        executor.addToBatch(row(RowKind.INSERT, 1, "b", 1));
        executor.addToBatch(row(RowKind.INSERT, 1, "a", 2));
        executor.executeBatch();

        Assertions.assertEquals(
                Arrays.asList(
                        "upsert[1, a, 1]",
                        "upsert[1, b, 1]",
                        "execute upsert",
                        "upsert[1, a, 2]",
                        "execute upsert"),
                log);
    }

    @Test
    void testDeletesAndUpsertsKeepArrivalOrder() throws SQLException {
        List<String> log = new ArrayList<>();
        JdbcBatchStatementExecutor<SeaTunnelRow> executor = newExecutor(log);

        executor.addToBatch(row(RowKind.INSERT, 1, "a", 1));
        executor.addToBatch(row(RowKind.UPDATE_BEFORE, 1, "a", 1));
        executor.addToBatch(row(RowKind.UPDATE_AFTER, 1, "b", 2));
        executor.addToBatch(row(RowKind.DELETE, 1, "b", 2));
        executor.addToBatch(row(RowKind.DELETE, 2, "x", 9));
        executor.addToBatch(row(RowKind.INSERT, 1, "c", 3));
        executor.executeBatch();

        Assertions.assertEquals(
                Arrays.asList(
                        "upsert[1, a, 1]",
                        "execute upsert",
                        "upsert[1, b, 2]",
                        "execute upsert",
                        "delete[1]",
                        "delete[2]",
                        "execute delete",
                        "upsert[1, c, 3]",
                        "execute upsert"),
                log);
    }

    @Test
    void testExecutedBatchIsNotSentAgain() throws SQLException {
        List<String> log = new ArrayList<>();
        JdbcBatchStatementExecutor<SeaTunnelRow> executor = newExecutor(log);

        executor.addToBatch(row(RowKind.INSERT, 1, "a", 1));
        executor.executeBatch();
        log.clear();
        executor.executeBatch();
        Assertions.assertEquals(Collections.emptyList(), log);

        // A new batch starts empty, so the same key is not treated as repeated.
        executor.addToBatch(row(RowKind.INSERT, 1, "b", 2));
        executor.executeBatch();
        Assertions.assertEquals(Arrays.asList("upsert[1, b, 2]", "execute upsert"), log);
    }

    @Test
    void testFailedFlushReplaysEveryRowOnceInOrder() throws SQLException {
        List<String> log = new ArrayList<>();
        FlakyExecutor upserts = new FlakyExecutor(log);
        JdbcBatchStatementExecutor<SeaTunnelRow> executor =
                new OrderedBufferedBatchStatementExecutor(
                        upserts,
                        new LoggingExecutor("delete", log),
                        keyById(),
                        Function.identity());

        executor.addToBatch(row(RowKind.INSERT, 1, "v300", 300));
        executor.addToBatch(row(RowKind.INSERT, 1, "v200", 200));
        upserts.failNextExecute = true;
        Assertions.assertThrows(SQLException.class, executor::executeBatch);

        // The failed attempt left a row in the statement batch; the retry clears it first.
        log.clear();
        executor.executeBatch();
        Assertions.assertEquals(
                Arrays.asList(
                        "clear",
                        "upsert[1, v300, 300]",
                        "execute upsert",
                        "upsert[1, v200, 200]",
                        "execute upsert"),
                log);
        Assertions.assertEquals(2, upserts.pending.size() + upserts.executed.size());
    }

    @Test
    void testClearBatchDropsBufferedRows() throws SQLException {
        List<String> log = new ArrayList<>();
        JdbcBatchStatementExecutor<SeaTunnelRow> executor = newExecutor(log);

        executor.addToBatch(row(RowKind.INSERT, 1, "a", 1));
        executor.clearBatch();
        log.clear();
        executor.executeBatch();
        executor.closeStatements();

        Assertions.assertEquals(Arrays.asList("close upsert", "close delete"), log);
    }

    @Test
    void testCloseFlushesPendingRows() throws SQLException {
        List<String> log = new ArrayList<>();
        JdbcBatchStatementExecutor<SeaTunnelRow> executor = newExecutor(log);

        executor.addToBatch(row(RowKind.INSERT, 1, "a", 1));
        executor.closeStatements();

        Assertions.assertEquals(
                Arrays.asList("upsert[1, a, 1]", "execute upsert", "close upsert", "close delete"),
                log);
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> newExecutor(List<String> log) {
        return new OrderedBufferedBatchStatementExecutor(
                new LoggingExecutor("upsert", log),
                new LoggingExecutor("delete", log),
                keyById(),
                Function.identity());
    }

    private static Function<SeaTunnelRow, SeaTunnelRow> keyById() {
        return row -> new SeaTunnelRow(new Object[] {row.getField(0)});
    }

    private static SeaTunnelRow row(RowKind kind, Integer id, String name, Integer version) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, name, version});
        row.setRowKind(kind);
        return row;
    }

    private static class LoggingExecutor implements JdbcBatchStatementExecutor<SeaTunnelRow> {
        private final String name;
        private final List<String> log;

        LoggingExecutor(String name, List<String> log) {
            this.name = name;
            this.log = log;
        }

        @Override
        public void prepareStatements(Connection connection) {}

        @Override
        public void addToBatch(SeaTunnelRow record) {
            log.add(name + Arrays.toString(record.getFields()));
        }

        @Override
        public void executeBatch() {
            log.add("execute " + name);
        }

        @Override
        public void closeStatements() {
            log.add("close " + name);
        }
    }

    private static class FlakyExecutor implements JdbcBatchStatementExecutor<SeaTunnelRow> {
        private final List<String> log;
        private final List<SeaTunnelRow> pending = new ArrayList<>();
        private final List<SeaTunnelRow> executed = new ArrayList<>();
        private boolean failNextExecute;

        FlakyExecutor(List<String> log) {
            this.log = log;
        }

        @Override
        public void prepareStatements(Connection connection) {}

        @Override
        public void addToBatch(SeaTunnelRow record) {
            log.add("upsert" + Arrays.toString(record.getFields()));
            pending.add(record);
        }

        @Override
        public void executeBatch() throws SQLException {
            if (failNextExecute) {
                failNextExecute = false;
                throw new SQLException("connection lost");
            }
            log.add("execute upsert");
            executed.addAll(pending);
            pending.clear();
        }

        @Override
        public void clearBatch() {
            log.add("clear");
            pending.clear();
        }

        @Override
        public void closeStatements() {}
    }

    /** Records added rows and always fails to execute, as the shared buffer test expects. */
    private static class FailingExecutor implements JdbcBatchStatementExecutor<SeaTunnelRow> {
        private final List<SeaTunnelRow> recorder;

        FailingExecutor(List<SeaTunnelRow> recorder) {
            this.recorder = recorder;
        }

        @Override
        public void prepareStatements(Connection connection) {}

        @Override
        public void addToBatch(SeaTunnelRow record) {
            recorder.add(record);
        }

        @Override
        public void executeBatch() throws SQLException {
            throw new SQLException("test");
        }

        @Override
        public void closeStatements() {}
    }
}

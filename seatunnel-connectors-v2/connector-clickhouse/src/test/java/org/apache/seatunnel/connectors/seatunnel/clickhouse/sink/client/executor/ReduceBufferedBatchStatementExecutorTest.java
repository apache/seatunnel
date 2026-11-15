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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client.executor;

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

class ReduceBufferedBatchStatementExecutorTest {

    @Test
    void preservesDistinctRowsWithNullKeys() throws SQLException {
        List<SeaTunnelRow> inserts = new ArrayList<>();
        ReduceBufferedBatchStatementExecutor executor =
                executor(inserts, new ArrayList<>(), row -> extractKey(row, 1));

        executor.addToBatch(row(RowKind.INSERT, 1, null, "first"));
        executor.addToBatch(row(RowKind.INSERT, 2, null, "second"));
        executor.executeBatch();

        Assertions.assertEquals(2, inserts.size());
        Assertions.assertEquals("first", inserts.get(0).getField(2));
        Assertions.assertEquals("second", inserts.get(1).getField(2));
    }

    @Test
    void preservesRowsWhenTheExtractorReturnsNoKey() throws SQLException {
        List<SeaTunnelRow> inserts = new ArrayList<>();
        ReduceBufferedBatchStatementExecutor executor =
                executor(inserts, new ArrayList<>(), row -> null);

        executor.addToBatch(row(RowKind.INSERT, 1, "first"));
        executor.addToBatch(row(RowKind.INSERT, 2, "second"));
        executor.executeBatch();

        Assertions.assertEquals(2, inserts.size());
    }

    @Test
    void preservesDistinctRowsWithAnIncompleteCompositeKey() throws SQLException {
        List<SeaTunnelRow> inserts = new ArrayList<>();
        ReduceBufferedBatchStatementExecutor executor =
                executor(inserts, new ArrayList<>(), row -> extractKey(row, 1, 2));

        executor.addToBatch(row(RowKind.INSERT, 1, null, "east", "first"));
        executor.addToBatch(row(RowKind.INSERT, 2, null, "east", "second"));
        executor.executeBatch();

        Assertions.assertEquals(2, inserts.size());
    }

    @Test
    void stillReducesCompleteKeysToTheLastChange() throws SQLException {
        List<SeaTunnelRow> inserts = new ArrayList<>();
        ReduceBufferedBatchStatementExecutor executor =
                executor(inserts, new ArrayList<>(), row -> extractKey(row, 1));

        executor.addToBatch(row(RowKind.INSERT, 1, 7, "first"));
        executor.addToBatch(row(RowKind.INSERT, 2, 7, "second"));
        executor.executeBatch();

        Assertions.assertEquals(1, inserts.size());
        Assertions.assertEquals("second", inserts.get(0).getField(2));
    }

    @Test
    void preservesDistinctDeletesWithNullKeys() throws SQLException {
        List<SeaTunnelRow> inserts = new ArrayList<>();
        List<SeaTunnelRow> deletes = new ArrayList<>();
        ReduceBufferedBatchStatementExecutor executor =
                executor(inserts, deletes, row -> extractKey(row, 1));

        executor.addToBatch(row(RowKind.DELETE, 1, null, "first"));
        executor.addToBatch(row(RowKind.DELETE, 2, null, "second"));
        executor.executeBatch();

        Assertions.assertTrue(inserts.isEmpty());
        Assertions.assertEquals(2, deletes.size());
        Assertions.assertNull(deletes.get(0).getField(0));
    }

    private static ReduceBufferedBatchStatementExecutor executor(
            List<SeaTunnelRow> inserts,
            List<SeaTunnelRow> deletes,
            Function<SeaTunnelRow, SeaTunnelRow> keyExtractor) {
        return new ReduceBufferedBatchStatementExecutor(
                new RecordingExecutor(inserts),
                new RecordingExecutor(deletes),
                keyExtractor,
                Function.identity(),
                false);
    }

    private static SeaTunnelRow extractKey(SeaTunnelRow row, int... fieldIndexes) {
        Object[] fields = new Object[fieldIndexes.length];
        for (int i = 0; i < fieldIndexes.length; i++) {
            fields[i] = row.getField(fieldIndexes[i]);
        }
        SeaTunnelRow key = new SeaTunnelRow(fields);
        key.setTableId(row.getTableId());
        key.setRowKind(row.getRowKind());
        return key;
    }

    private static SeaTunnelRow row(RowKind kind, Object... fields) {
        SeaTunnelRow row = new SeaTunnelRow(fields);
        row.setRowKind(kind);
        return row;
    }

    private static class RecordingExecutor implements JdbcBatchStatementExecutor {
        private final List<SeaTunnelRow> rows;

        private RecordingExecutor(List<SeaTunnelRow> rows) {
            this.rows = rows;
        }

        @Override
        public void prepareStatements(Connection connection) {}

        @Override
        public void addToBatch(SeaTunnelRow row) {
            rows.add(row);
        }

        @Override
        public void executeBatch() {}

        @Override
        public void closeStatements() {}
    }
}

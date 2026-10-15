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
        List<String> upserts = new ArrayList<>();
        List<String> deletes = new ArrayList<>();
        BufferReducedBatchStatementExecutor executor =
                new BufferReducedBatchStatementExecutor(
                        recording(upserts),
                        recording(deletes),
                        r -> new SeaTunnelRow(new Object[] {r.getField(0)}),
                        Function.identity());
        // Created then updated in one batch: still has to be inserted.
        executor.addToBatch(row(RowKind.INSERT, 1, "a"));
        executor.addToBatch(row(RowKind.UPDATE_BEFORE, 1, "a"));
        executor.addToBatch(row(RowKind.UPDATE_AFTER, 1, "b"));
        // Existing row updated twice.
        executor.addToBatch(row(RowKind.UPDATE_AFTER, 2, "x"));
        executor.addToBatch(row(RowKind.UPDATE_AFTER, 2, "y"));
        // Existing row deleted and re-inserted: the row is still there, so update it.
        executor.addToBatch(row(RowKind.DELETE, 3, "p"));
        executor.addToBatch(row(RowKind.INSERT, 3, "q"));
        // Existing row updated then deleted.
        executor.addToBatch(row(RowKind.UPDATE_AFTER, 4, "m"));
        executor.addToBatch(row(RowKind.DELETE, 4, "m"));
        executor.executeBatch();

        Assertions.assertEquals(Arrays.asList("+I[1, b]", "+U[2, y]", "+U[3, q]"), upserts);
        Assertions.assertEquals(Collections.singletonList("+I[4]"), deletes);

        // A new batch judges existence afresh.
        upserts.clear();
        executor.addToBatch(row(RowKind.UPDATE_AFTER, 1, "c"));
        executor.executeBatch();
        Assertions.assertEquals(Collections.singletonList("+U[1, c]"), upserts);
    }

    private static SeaTunnelRow row(RowKind kind, int id, String value) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, value});
        row.setRowKind(kind);
        return row;
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> recording(List<String> recorder) {
        return new JdbcBatchStatementExecutor<SeaTunnelRow>() {
            @Override
            public void prepareStatements(Connection connection) {}

            @Override
            public void addToBatch(SeaTunnelRow record) {
                recorder.add(
                        record.getRowKind().shortString() + Arrays.toString(record.getFields()));
            }

            @Override
            public void executeBatch() {}

            @Override
            public void closeStatements() {}
        };
    }
}

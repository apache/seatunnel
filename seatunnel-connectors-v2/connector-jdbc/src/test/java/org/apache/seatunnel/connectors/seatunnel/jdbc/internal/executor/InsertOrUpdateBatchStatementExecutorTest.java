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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class InsertOrUpdateBatchStatementExecutorTest {

    @Test
    void clearBatchClearsPreparedStatementsAndPreventsCloseFlush() throws Exception {
        StatementRecorder insertRecorder = new StatementRecorder();
        StatementRecorder updateRecorder = new StatementRecorder();
        InsertOrUpdateBatchStatementExecutor executor =
                new InsertOrUpdateBatchStatementExecutor(
                        ignored -> insertRecorder.statement(),
                        ignored -> updateRecorder.statement(),
                        TableSchema.builder().build(),
                        null,
                        new NoopJdbcRowConverter());

        executor.prepareStatements(null);
        executor.addToBatch(new SeaTunnelRow(new Object[] {1}));
        executor.clearBatch();
        executor.closeStatements();

        assertEquals(1, insertRecorder.addBatchCount);
        assertEquals(1, insertRecorder.clearBatchCount);
        assertEquals(0, insertRecorder.executeBatchCount);
        assertEquals(1, insertRecorder.closeCount);
        assertEquals(1, updateRecorder.clearBatchCount);
        assertEquals(0, updateRecorder.executeBatchCount);
        assertEquals(1, updateRecorder.closeCount);
    }

    private static final class StatementRecorder {
        private int addBatchCount;
        private int clearBatchCount;
        private int executeBatchCount;
        private int closeCount;
        private PreparedStatement statement;

        private PreparedStatement statement() {
            if (statement == null) {
                statement =
                        (PreparedStatement)
                                Proxy.newProxyInstance(
                                        PreparedStatement.class.getClassLoader(),
                                        new Class<?>[] {PreparedStatement.class},
                                        (proxy, method, args) -> {
                                            switch (method.getName()) {
                                                case "addBatch":
                                                    addBatchCount++;
                                                    return null;
                                                case "clearBatch":
                                                    clearBatchCount++;
                                                    return null;
                                                case "executeBatch":
                                                    executeBatchCount++;
                                                    return new int[0];
                                                case "close":
                                                    closeCount++;
                                                    return null;
                                                case "isClosed":
                                                    return false;
                                                default:
                                                    return defaultValue(method.getReturnType());
                                            }
                                        });
            }
            return statement;
        }
    }

    private static Object defaultValue(Class<?> returnType) {
        if (returnType == Void.TYPE) {
            return null;
        }
        if (returnType == Boolean.TYPE) {
            return false;
        }
        if (returnType == Byte.TYPE) {
            return (byte) 0;
        }
        if (returnType == Short.TYPE) {
            return (short) 0;
        }
        if (returnType == Integer.TYPE) {
            return 0;
        }
        if (returnType == Long.TYPE) {
            return 0L;
        }
        if (returnType == Float.TYPE) {
            return 0F;
        }
        if (returnType == Double.TYPE) {
            return 0D;
        }
        if (returnType == Character.TYPE) {
            return (char) 0;
        }
        return null;
    }

    private static final class NoopJdbcRowConverter implements JdbcRowConverter {

        @Override
        public SeaTunnelRow toInternal(ResultSet rs, TableSchema tableSchema) {
            return null;
        }

        @Override
        public PreparedStatement toExternal(
                TableSchema tableSchema, SeaTunnelRow row, PreparedStatement statement) {
            return statement;
        }

        @Override
        public PreparedStatement toExternal(
                TableSchema tableSchema,
                TableSchema databaseTableSchema,
                SeaTunnelRow row,
                PreparedStatement statement) {
            assertSame(statement, toExternal(tableSchema, row, statement));
            return statement;
        }
    }
}

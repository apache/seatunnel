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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.sql.Connection;
import java.sql.SQLException;
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
}

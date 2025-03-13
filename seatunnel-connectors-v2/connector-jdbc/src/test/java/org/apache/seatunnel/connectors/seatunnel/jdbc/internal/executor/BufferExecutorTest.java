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
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.TestConnection;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public abstract class BufferExecutorTest {

    abstract JdbcBatchStatementExecutor<SeaTunnelRow> getExecutorWithBatchRecorder(
            List<SeaTunnelRow> recorder);

    @Test
    void testCacheAlwaysExistWhenInsertFailed() throws SQLException {
        List<SeaTunnelRow> recorder = new ArrayList<>();

        JdbcBatchStatementExecutor<SeaTunnelRow> executor = getExecutorWithBatchRecorder(recorder);
        executor.prepareStatements(new TestConnection());
        executor.addToBatch(new SeaTunnelRow(new Object[] {"test"}));

        SQLException exception =
                Assertions.assertThrows(SQLException.class, executor::executeBatch);
        Assertions.assertEquals("test", exception.getMessage());
        // the main point of this test is to check if the buffer is cleared after closeStatements
        // and prepareStatements when executeBatch failed
        Assertions.assertThrows(SQLException.class, executor::closeStatements);
        executor.prepareStatements(new TestConnection());
        SQLException exception2 =
                Assertions.assertThrows(SQLException.class, executor::executeBatch);
        Assertions.assertEquals("test", exception2.getMessage());

        // three times of addToBatch, 1. executeBatch, 2. closeStatements, 3. executeBatch
        Assertions.assertEquals(3, recorder.size());
        // same row to executeBatch
        Assertions.assertEquals(recorder.get(0), recorder.get(2));
    }
}

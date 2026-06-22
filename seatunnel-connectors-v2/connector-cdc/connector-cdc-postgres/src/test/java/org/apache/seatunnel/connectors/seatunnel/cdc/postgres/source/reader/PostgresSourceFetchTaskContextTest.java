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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.reader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

public class PostgresSourceFetchTaskContextTest {

    @Test
    public void testReplicationSlotAlreadyExistsWithSqlState() {
        SQLException exception = new SQLException("错误: 复制槽名 \"seatunnel\" 已经存在", "42710");

        Assertions.assertTrue(
                PostgresSourceFetchTaskContext.isReplicationSlotAlreadyExists(exception));
    }

    @Test
    public void testReplicationSlotAlreadyExistsWithEnglishMessage() {
        SQLException exception =
                new SQLException("ERROR: replication slot \"seatunnel\" already exists");

        Assertions.assertTrue(
                PostgresSourceFetchTaskContext.isReplicationSlotAlreadyExists(exception));
    }

    @Test
    public void testReplicationSlotCreateOtherFailure() {
        SQLException exception =
                new SQLException("ERROR: all replication slots are in use", "53400");

        Assertions.assertFalse(
                PostgresSourceFetchTaskContext.isReplicationSlotAlreadyExists(exception));
    }
}

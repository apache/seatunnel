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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.offset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.Lsn;

import java.util.HashMap;
import java.util.Map;

class LsnOffsetTest {

    @Test
    void testNoStoppingOffsetIsNeverStop() {
        Assertions.assertTrue(LsnOffset.NO_STOPPING_OFFSET.isNeverStop());
        Assertions.assertFalse(LsnOffset.INITIAL_OFFSET.isNeverStop());
    }

    @Test
    void testGetLsnCommitWhenLsnCommitKeyExists() {
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(SourceInfo.LSN_KEY, "12345");
        offsetMap.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, "67890");
        LsnOffset offset = new LsnOffset(offsetMap);

        Lsn lsnCommit = offset.getLsnCommit();
        Assertions.assertEquals(Lsn.valueOf(67890L), lsnCommit);
    }

    @Test
    void testGetLsnCommitFallbackToLsnWhenLsnCommitKeyMissing() {
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(SourceInfo.LSN_KEY, "12345");
        LsnOffset offset = new LsnOffset(offsetMap);

        Lsn lsnCommit = offset.getLsnCommit();
        Assertions.assertEquals(Lsn.valueOf(12345L), lsnCommit);
    }

    @Test
    void testConstructorStoresLastCommitLsnKey() {
        // Verify that the (Long, Long, Instant) constructor stores
        // LAST_COMMIT_LSN_KEY so that savepoint recovery has a valid
        // commit LSN even before the first commitCurrentOffset call.
        LsnOffset offset = new LsnOffset(12345L, null, null);
        Lsn lsnCommit = offset.getLsnCommit();
        Assertions.assertEquals(Lsn.valueOf(12345L), lsnCommit);
    }
}

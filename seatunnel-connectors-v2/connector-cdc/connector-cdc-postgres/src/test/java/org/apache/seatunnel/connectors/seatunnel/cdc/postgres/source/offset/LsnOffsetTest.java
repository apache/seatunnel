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

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.connector.postgresql.connection.ReplicationMessage.Operation;
import io.debezium.connector.postgresql.connection.WalPositionLocator;
import io.debezium.relational.TableId;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    @Test
    void testRestoreCommitDoesNotSkipInsertAtSameLsn() {
        assertFirstInsertReplayed(Operation.COMMIT);
    }

    @Test
    void testRestoreBeginDoesNotSkipFirstInsert() {
        assertFirstInsertReplayed(Operation.BEGIN);
    }

    @Test
    void testLegacyOffsetWithoutOperationReplaysAmbiguousBoundary() {
        assertFirstInsertReplayed(null);
    }

    @Test
    void testRestoreProcessedInsertResumesAtNextRecord() {
        Lsn stored = Lsn.valueOf(123L);
        Lsn next = Lsn.valueOf(124L);
        WalPositionLocator locator =
                new WalPositionLocator(Lsn.valueOf(100L), stored, Operation.INSERT);
        Assertions.assertFalse(locator.resumeFromLsn(stored, message(Operation.BEGIN)).isPresent());
        Assertions.assertFalse(
                locator.resumeFromLsn(stored, message(Operation.INSERT)).isPresent());
        Assertions.assertEquals(
                Optional.of(next), locator.resumeFromLsn(next, message(Operation.UPDATE)));
        locator.enableFiltering();
        Assertions.assertTrue(locator.skipMessage(stored));
        Assertions.assertFalse(locator.skipMessage(next));
    }

    @Test
    void testOffsetOperationSurvivesCheckpointAndChangesAtCommit() {
        PostgresOffsetContext.Loader loader = offsetLoader();
        PostgresOffsetContext context = loader.load(legacyOffset());
        Assertions.assertFalse(
                context.getOffset()
                        .containsKey(PostgresOffsetContext.LAST_PROCESSED_MESSAGE_TYPE_KEY));
        context.updateWalPosition(
                Lsn.valueOf(123L),
                Lsn.valueOf(123L),
                Instant.EPOCH,
                7L,
                null,
                TableId.parse("postgres.public.test"),
                Operation.INSERT);
        Assertions.assertEquals(
                "INSERT",
                loader.load(context.getOffset())
                        .getOffset()
                        .get(PostgresOffsetContext.LAST_PROCESSED_MESSAGE_TYPE_KEY));

        // The default non-transactional-metadata path updates the commit independently.
        context.updateCommitPosition(Lsn.valueOf(124L), Lsn.valueOf(124L));
        PostgresOffsetContext restored = loader.load(context.getOffset());
        Assertions.assertEquals(
                "COMMIT",
                restored.getOffset().get(PostgresOffsetContext.LAST_PROCESSED_MESSAGE_TYPE_KEY));
        Assertions.assertEquals(
                124L,
                restored.getOffset().get(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY));
    }

    private void assertFirstInsertReplayed(Operation previousOperation) {
        Lsn boundary = Lsn.valueOf(123L);
        WalPositionLocator locator =
                previousOperation == null
                        ? new WalPositionLocator(boundary, boundary)
                        : new WalPositionLocator(boundary, boundary, previousOperation);
        Assertions.assertFalse(
                locator.resumeFromLsn(boundary, message(Operation.BEGIN)).isPresent());
        Assertions.assertEquals(
                Optional.of(boundary), locator.resumeFromLsn(boundary, message(Operation.INSERT)));
        locator.enableFiltering();
        Assertions.assertFalse(locator.skipMessage(boundary));
    }

    private ReplicationMessage message(Operation operation) {
        ReplicationMessage message = mock(ReplicationMessage.class);
        when(message.getOperation()).thenReturn(operation);
        return message;
    }

    private PostgresOffsetContext.Loader offsetLoader() {
        return new PostgresOffsetContext.Loader(
                new PostgresConnectorConfig(
                        Configuration.create()
                                .with("database.server.name", "offset-test")
                                .with("database.hostname", "localhost")
                                .with("database.user", "test")
                                .with("database.dbname", "postgres")
                                .build()));
    }

    private Map<String, Object> legacyOffset() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.LSN_KEY, 123L);
        offset.put(SourceInfo.TIMESTAMP_USEC_KEY, 0L);
        offset.put(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY, 123L);
        offset.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, 100L);
        return offset;
    }
}

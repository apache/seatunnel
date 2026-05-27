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

package org.apache.seatunnel.edge.agent.starter.wal;

import org.apache.seatunnel.edge.agent.connector.EdgeEvent;
import org.apache.seatunnel.edge.agent.starter.wal.sqlite.SqliteWalStore;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SqliteWalStoreTest {

    private static final int MAX_ATTEMPTS = 3;

    @TempDir Path tempDir;

    @Test
    void appendClaimAckAndResurrectSending() throws Exception {
        Path dbPath = tempDir.resolve("wal.db");
        try (SqliteWalStore store = new SqliteWalStore(dbPath)) {
            long id =
                    store.append(
                            EdgeEvent.builder()
                                    .sourceId("src-1")
                                    .payload(new byte[] {1, 2, 3})
                                    .eventTime(100L)
                                    .build());

            List<WalRecord> claimed = store.claimPending(10, MAX_ATTEMPTS);
            Assertions.assertEquals(1, claimed.size());
            Assertions.assertEquals(id, claimed.get(0).getId());
            Assertions.assertEquals(1L, claimed.get(0).getBatchId());
            Assertions.assertEquals(WalRecordStatus.SENDING, claimed.get(0).getStatus());

            store.ack(id);

            List<WalRecord> afterAck = store.claimPending(10, MAX_ATTEMPTS);
            Assertions.assertTrue(afterAck.isEmpty());
        }
    }

    @Test
    void resurrectSendingMovesRecordsBackToPending() throws Exception {
        Path dbPath = tempDir.resolve("wal-resurrect.db");
        try (SqliteWalStore store = new SqliteWalStore(dbPath)) {
            store.append(
                    EdgeEvent.builder()
                            .sourceId("src-1")
                            .payload(new byte[] {9})
                            .eventTime(200L)
                            .build());

            Assertions.assertEquals(1, store.claimPending(10, MAX_ATTEMPTS).size());
            Assertions.assertEquals(1, store.resurrectSending(10));

            List<WalRecord> reclaimed = store.claimPending(10, MAX_ATTEMPTS);
            Assertions.assertEquals(1, reclaimed.size());
            Assertions.assertEquals(WalRecordStatus.SENDING, reclaimed.get(0).getStatus());
        }
    }

    @Test
    void markExceededAsDeadStopsClaiming() throws Exception {
        Path dbPath = tempDir.resolve("wal-dead.db");
        try (SqliteWalStore store = new SqliteWalStore(dbPath)) {
            long id =
                    store.append(
                            EdgeEvent.builder()
                                    .sourceId("src-1")
                                    .payload(new byte[] {1})
                                    .eventTime(1L)
                                    .build());

            for (int i = 0; i < MAX_ATTEMPTS; i++) {
                List<WalRecord> claimed = store.claimPending(10, MAX_ATTEMPTS);
                Assertions.assertEquals(1, claimed.size());
                Assertions.assertEquals(id, claimed.get(0).getId());
                if (i < MAX_ATTEMPTS - 1) {
                    store.resurrectSending(10);
                }
            }

            Assertions.assertTrue(store.claimPending(10, MAX_ATTEMPTS).isEmpty());

            store.resurrectSending(10);
            Assertions.assertEquals(1, store.markExceededAsDead(MAX_ATTEMPTS, 10));
            Assertions.assertTrue(store.claimPending(10, MAX_ATTEMPTS).isEmpty());
        }
    }

    @Test
    void appendAssignsMonotonicBatchIds() throws Exception {
        Path dbPath = tempDir.resolve("wal-batch-id.db");
        try (SqliteWalStore store = new SqliteWalStore(dbPath)) {
            store.append(
                    EdgeEvent.builder()
                            .sourceId("src-1")
                            .payload(new byte[] {1})
                            .eventTime(1L)
                            .build());
            store.append(
                    EdgeEvent.builder()
                            .sourceId("src-1")
                            .payload(new byte[] {2})
                            .eventTime(2L)
                            .build());

            List<WalRecord> claimed = store.claimPending(10, MAX_ATTEMPTS);
            Assertions.assertEquals(2, claimed.size());
            Assertions.assertEquals(1L, claimed.get(0).getBatchId());
            Assertions.assertEquals(2L, claimed.get(1).getBatchId());
        }
    }

    @Test
    void cleanupAckedDeletesOnlyEligibleRows() throws Exception {
        Path dbPath = tempDir.resolve("wal-cleanup.db");
        long retentionMs = 150L;
        try (SqliteWalStore store = new SqliteWalStore(dbPath)) {
            long oldId =
                    store.append(
                            EdgeEvent.builder()
                                    .sourceId("src-1")
                                    .payload(new byte[] {1})
                                    .eventTime(1L)
                                    .build());
            store.claimPending(10, MAX_ATTEMPTS);
            long ackedAt = System.currentTimeMillis();
            store.ack(oldId);

            Awaitility.await()
                    .atMost(5, TimeUnit.SECONDS)
                    .pollInterval(20, TimeUnit.MILLISECONDS)
                    .until(() -> System.currentTimeMillis() - ackedAt >= retentionMs);

            long recentId =
                    store.append(
                            EdgeEvent.builder()
                                    .sourceId("src-1")
                                    .payload(new byte[] {2})
                                    .eventTime(2L)
                                    .build());
            store.claimPending(10, MAX_ATTEMPTS);
            store.ack(recentId);

            int deleted = store.cleanupAcked(retentionMs, 10);
            Assertions.assertEquals(1, deleted);

            int secondPass = store.cleanupAcked(retentionMs, 10);
            Assertions.assertEquals(0, secondPass);
        }
    }
}

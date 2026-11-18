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

package org.apache.seatunnel.benchmark.multitablesink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(30)
class MultiTableSinkFixtureTest {

    @Test
    void shouldRejectDuplicateRowsEvenWhenTheRollingChecksumMatches() throws Exception {
        MultiTableSinkFixture fixture = fixture(1, 1, MultiTableSinkFixture.Routing.HASH, 1000, 0);
        fixture.setUp();
        Field field = MultiTableSinkFixture.class.getDeclaredField("records");
        field.setAccessible(true);
        SeaTunnelRow[] records = (SeaTunnelRow[]) field.get(fixture);
        // Replace rows 1 and 2 with duplicates of 0 and 33. Counts and the old checksum
        // remain unchanged: (0 - 1) * 31 + (33 - 2) == 0.
        records[1] = records[0];
        records[2] = records[33];
        try {
            IOException failure =
                    assertThrows(
                            IOException.class,
                            () -> {
                                publish(fixture, 34);
                                fixture.drainAndVerify();
                            });
            Throwable cause = failure;
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }
            assertTrue(cause.getMessage().contains("expected=1, actual=0"));
        } finally {
            try {
                fixture.close();
            } catch (IOException | RuntimeException expected) {
                // A failed queue worker is also reported during shutdown.
            }
            assertTrue(fixture.isFullyClosed());
        }
    }

    @Test
    void shouldDrainAndVerifySingleAndMultipleTablesAcrossReplicas() throws Exception {
        for (int tables : new int[] {1, 4}) {
            for (int replicas : new int[] {1, 5, 15}) {
                MultiTableSinkFixture fixture =
                        fixture(replicas, tables, MultiTableSinkFixture.Routing.HASH, 1000, 0);
                fixture.setUp();
                try {
                    publish(fixture, 20_003);
                    fixture.drainAndVerify();
                    assertEquals(20_003, fixture.getPublishedRows());
                    assertEquals(20_003, fixture.getConsumedRows());
                    for (int table = 0; table < tables; table++) {
                        for (int replica = 0; replica < replicas; replica++) {
                            assertTrue(fixture.getConsumedRows(table, replica) > 0);
                        }
                    }
                } finally {
                    fixture.close();
                }
                assertTrue(fixture.isFullyClosed());
            }
        }
    }

    @Test
    void shouldDrainPoolWrapsAndFlushPartialBatchesAcrossIterations() throws Exception {
        MultiTableSinkFixture fixture = fixture(5, 4, MultiTableSinkFixture.Routing.HASH, 64, 100);
        fixture.setUp();
        try {
            publish(fixture, 65_537);
            fixture.drainAndVerify();
            assertEquals(1040, fixture.getBatchCount());
            publish(fixture, 65_540);
            fixture.drainAndVerify();
            assertEquals(131_077, fixture.getConsumedRows());
            assertEquals(2080, fixture.getBatchCount());
            fixture.drainAndVerify();
            assertEquals(2080, fixture.getBatchCount());
        } finally {
            fixture.close();
        }
    }

    @Test
    void shouldKeepSingleQueueControlOnReplicaZeroForEveryTable() throws Exception {
        MultiTableSinkFixture fixture =
                fixture(15, 4, MultiTableSinkFixture.Routing.SINGLE_QUEUE, 1000, 0);
        fixture.setUp();
        try {
            publish(fixture, 20_003);
            fixture.drainAndVerify();
            for (int table = 0; table < 4; table++) {
                assertTrue(fixture.getConsumedRows(table, 0) > 0);
                for (int replica = 1; replica < 15; replica++) {
                    assertEquals(0, fixture.getConsumedRows(table, replica));
                }
            }
        } finally {
            fixture.close();
        }
    }

    private static MultiTableSinkFixture fixture(
            int replicas,
            int tables,
            MultiTableSinkFixture.Routing routing,
            int batchSize,
            long delayMicros) {
        return new MultiTableSinkFixture(replicas, tables, 65536, routing, batchSize, delayMicros);
    }

    private static void publish(MultiTableSinkFixture fixture, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            fixture.publish();
        }
    }
}

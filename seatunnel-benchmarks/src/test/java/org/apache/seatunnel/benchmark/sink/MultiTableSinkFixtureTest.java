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

package org.apache.seatunnel.benchmark.sink;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(30)
class MultiTableSinkFixtureTest {

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
    void shouldVerifyOrderAcrossPoolWrapsAndIterationBoundaries() throws Exception {
        MultiTableSinkFixture fixture = fixture(5, 4, MultiTableSinkFixture.Routing.HASH, 1000, 0);
        fixture.setUp();
        try {
            publish(fixture, 65_537);
            fixture.drainAndVerify();
            publish(fixture, 65_540);
            fixture.drainAndVerify();
            assertEquals(131_077, fixture.getConsumedRows());
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

    @Test
    void shouldFlushPartialDelayedBatchesAndAvoidEmptyFlushWork() throws Exception {
        MultiTableSinkFixture fixture = fixture(5, 1, MultiTableSinkFixture.Routing.HASH, 64, 100);
        fixture.setUp();
        try {
            publish(fixture, 1003);
            fixture.drainAndVerify();
            assertEquals(1003, fixture.getConsumedRows());
            // Each replica receives 200 or 201 rows, requiring three full and one tail batch.
            assertEquals(20, fixture.getBatchCount());
            fixture.drainAndVerify();
            assertEquals(20, fixture.getBatchCount());
        } finally {
            fixture.close();
        }
    }

    @Test
    void shouldCloseAnEmptyFixtureAndAllowRepeatedClose() throws Exception {
        MultiTableSinkFixture fixture = fixture(5, 1, MultiTableSinkFixture.Routing.HASH, 1000, 0);
        fixture.setUp();
        fixture.close();
        fixture.close();
        assertTrue(fixture.isFullyClosed());
    }

    @Test
    void shouldRejectParametersThatCannotRepresentTheWorkload() {
        assertThrows(
                IllegalArgumentException.class,
                () -> fixture(0, 1, MultiTableSinkFixture.Routing.HASH, 1000, 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> fixture(1, 0, MultiTableSinkFixture.Routing.HASH, 1000, 0));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new MultiTableSinkFixture(
                                15, 1, 1024, MultiTableSinkFixture.Routing.HASH, 1000, 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> fixture(1, 1, MultiTableSinkFixture.Routing.HASH, 0, 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> fixture(1, 1, MultiTableSinkFixture.Routing.HASH, 1000, -1));
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

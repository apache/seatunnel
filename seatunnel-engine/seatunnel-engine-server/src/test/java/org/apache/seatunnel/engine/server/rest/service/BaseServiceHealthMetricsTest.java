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

package org.apache.seatunnel.engine.server.rest.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Verifies the member-collection contract of {@link
 * BaseService#getSystemMonitoringInformationJsonValues()}: all members share one deadline, and
 * members whose metrics are not collected are still reported with their address and an error marker
 * instead of an anonymous empty object.
 */
public class BaseServiceHealthMetricsTest {

    private static Address address(int port) throws UnknownHostException {
        return new Address("127.0.0.1", port);
    }

    private static InternalCompletableFuture<Object> future(Object value) {
        InternalCompletableFuture<Object> future = new InternalCompletableFuture<>();
        if (value != null) {
            future.complete(value);
        }
        return future;
    }

    /** Avoids the live SeaTunnelServer config lookup by pinning a short timeout. */
    private static final class TestService extends BaseService {

        private TestService() {
            super(null);
        }

        @Override
        int getHealthMetricsTimeoutSeconds() {
            return 1;
        }
    }

    private static void assertTimeoutEntry(
            JsonObject entry, String expectedHost, int expectedPort) {
        Assertions.assertEquals(expectedHost, entry.get("host").asString());
        Assertions.assertEquals(expectedPort, entry.get("port").asInt());
        Assertions.assertEquals("timeout", entry.get("error").asString());
    }

    @Test
    public void testTimedOutMemberIsReportedWithAddressAndErrorMarker() throws Exception {
        Map<Address, InternalCompletableFuture<Object>> futures = new LinkedHashMap<>();
        futures.put(address(5801), future(null));

        JsonArray values = new TestService().collectHealthMetrics(futures);

        Assertions.assertEquals(1, values.size());
        assertTimeoutEntry(values.get(0).asObject(), "127.0.0.1", 5801);
    }

    @Test
    public void testSharedDeadlineBoundsTotalWaitForAllMembers() throws Exception {
        Map<Address, InternalCompletableFuture<Object>> futures = new LinkedHashMap<>();
        futures.put(address(5801), future(null));
        futures.put(address(5802), future(null));
        futures.put(address(5803), future(null));
        futures.put(
                address(5804),
                future("isMaster=true, host=127.0.0.1, port=5804, heap.memory.used=135.7M"));

        long startNanos = System.nanoTime();
        JsonArray values = new TestService().collectHealthMetrics(futures);
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        Assertions.assertEquals(4, values.size());
        assertTimeoutEntry(values.get(0).asObject(), "127.0.0.1", 5801);
        assertTimeoutEntry(values.get(1).asObject(), "127.0.0.1", 5802);
        assertTimeoutEntry(values.get(2).asObject(), "127.0.0.1", 5803);
        JsonObject collected = values.get(3).asObject();
        Assertions.assertEquals("true", collected.get("isMaster").asString());
        Assertions.assertEquals("135.7M", collected.get("heap.memory.used").asString());
        // a per-member deadline would wait 3 x timeout for the three hanging members
        Assertions.assertTrue(
                elapsedMillis < 2400,
                "shared deadline should bound the total wait, took " + elapsedMillis + "ms");
    }

    @Test
    public void testInterruptStopsWaitingForRemainingMembers() throws Exception {
        Map<Address, InternalCompletableFuture<Object>> futures = new LinkedHashMap<>();
        InternalCompletableFuture<Object> hanging = future(null);
        futures.put(address(5801), hanging);
        futures.put(address(5802), future("isMaster=true, host=127.0.0.1, port=5802"));

        Thread.currentThread().interrupt();
        try {
            JsonArray values = new TestService().collectHealthMetrics(futures);
            Assertions.assertEquals(0, values.size());
            Assertions.assertTrue(hanging.isDone() && hanging.isCancelled());
        } finally {
            // the collector must have restored the interrupt flag; clear it for other tests
            Assertions.assertTrue(Thread.interrupted());
        }
    }

    @Test
    public void testFailedResponseIsReportedWithErrorMarker() throws Exception {
        Map<Address, InternalCompletableFuture<Object>> futures = new LinkedHashMap<>();
        InternalCompletableFuture<Object> failed = future(null);
        failed.completeExceptionally(new RuntimeException("boom"));
        futures.put(address(5801), failed);

        JsonArray values = new TestService().collectHealthMetrics(futures);

        Assertions.assertEquals(1, values.size());
        JsonObject entry = values.get(0).asObject();
        Assertions.assertEquals("127.0.0.1", entry.get("host").asString());
        Assertions.assertEquals(5801, entry.get("port").asInt());
        Assertions.assertEquals("execution-failure", entry.get("error").asString());
    }

    @Test
    public void testDispatchFailureIsReportedWithErrorMarker() throws Exception {
        Map<Address, InternalCompletableFuture<Object>> futures = new LinkedHashMap<>();
        futures.put(address(5801), null);

        JsonArray values = new TestService().collectHealthMetrics(futures);

        Assertions.assertEquals(1, values.size());
        JsonObject entry = values.get(0).asObject();
        Assertions.assertEquals("127.0.0.1", entry.get("host").asString());
        Assertions.assertEquals(5801, entry.get("port").asInt());
        Assertions.assertEquals("dispatch-failure", entry.get("error").asString());
    }
}

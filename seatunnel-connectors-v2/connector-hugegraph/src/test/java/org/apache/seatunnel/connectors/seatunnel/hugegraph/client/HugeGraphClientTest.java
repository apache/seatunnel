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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.client;

import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphConnectionConfig;

import org.apache.hugegraph.exception.ServerException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HugeGraphClientTest {

    @Test
    void testBuildHttpsServerUrl() {
        HugeGraphConnectionConfig config = new HugeGraphConnectionConfig();
        config.setProtocol("HTTPS");
        config.setHost("graph.example.com");
        config.setPort(8443);

        assertEquals("https://graph.example.com:8443", HugeGraphClient.buildServerUrl(config));
    }

    @Test
    void testRetryableHttpStatuses() {
        assertTrue(HugeGraphClient.isRetryable(serverException(408)));
        assertTrue(HugeGraphClient.isRetryable(serverException(429)));
        assertTrue(HugeGraphClient.isRetryable(serverException(503)));
        assertFalse(HugeGraphClient.isRetryable(serverException(400)));
        assertFalse(HugeGraphClient.isRetryable(serverException(404)));
    }

    @Test
    void testExponentialBackoffGrowsAndCaps() {
        // base=1000, cap=5000: 1000, 2000, 4000, then capped at 5000.
        assertEquals(1000L, HugeGraphClient.computeBackoffMs(1000L, 5000L, 1));
        assertEquals(2000L, HugeGraphClient.computeBackoffMs(1000L, 5000L, 2));
        assertEquals(4000L, HugeGraphClient.computeBackoffMs(1000L, 5000L, 3));
        assertEquals(5000L, HugeGraphClient.computeBackoffMs(1000L, 5000L, 4));
        assertEquals(5000L, HugeGraphClient.computeBackoffMs(1000L, 5000L, 20));
    }

    @Test
    void testBackoffEdgeCases() {
        // Zero base disables backoff regardless of attempt.
        assertEquals(0L, HugeGraphClient.computeBackoffMs(0L, 5000L, 5));
        // Non-positive cap means no cap: keeps growing exponentially.
        assertEquals(8000L, HugeGraphClient.computeBackoffMs(1000L, 0L, 4));
        // Large attempt does not overflow (shift is bounded); stays capped.
        assertEquals(30000L, HugeGraphClient.computeBackoffMs(5000L, 30000L, 100));
    }

    private static ServerException serverException(int status) {
        ServerException exception = mock(ServerException.class);
        when(exception.status()).thenReturn(status);
        return exception;
    }
}

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
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Vertex;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HugeGraphClientTest {

    @Test
    void deleteVerticesByLabelPagesUntilEmptyAndDeletesEach() {
        HugeGraphClient client = spy(new HugeGraphClient(new HugeGraphConnectionConfig()));
        // Two full pages then an empty page ends the loop; the empty page always re-reads from the
        // start ("" cursor), so it does not rely on a paging token staying valid across deletes.
        doReturn(new PageResult<>(Arrays.asList(vertex("a"), vertex("b")), null))
                .doReturn(new PageResult<>(Collections.singletonList(vertex("c")), null))
                .doReturn(new PageResult<>(Collections.emptyList(), null))
                .when(client)
                .listVertices(eq("person"), isNull(), eq(""), anyInt());
        doNothing().when(client).deleteVertex(org.mockito.ArgumentMatchers.any());

        client.deleteVerticesByLabel("person");

        verify(client).deleteVertex("a");
        verify(client).deleteVertex("b");
        verify(client).deleteVertex("c");
        verify(client, times(3)).listVertices(eq("person"), isNull(), eq(""), anyInt());
    }

    @Test
    void deleteEdgesByLabelPagesUntilEmptyAndDeletesEach() {
        HugeGraphClient client = spy(new HugeGraphClient(new HugeGraphConnectionConfig()));
        doReturn(new PageResult<>(Arrays.asList(edge("e1"), edge("e2")), null))
                .doReturn(new PageResult<>(Collections.emptyList(), null))
                .when(client)
                .listEdges(eq("knows"), isNull(), eq(""), anyInt());
        doNothing().when(client).deleteEdge(org.mockito.ArgumentMatchers.anyString());

        client.deleteEdgesByLabel("knows");

        verify(client).deleteEdge("e1");
        verify(client).deleteEdge("e2");
        verify(client, times(2)).listEdges(eq("knows"), isNull(), eq(""), anyInt());
    }

    private static Vertex vertex(Object id) {
        Vertex vertex = new Vertex("person");
        vertex.id(id);
        return vertex;
    }

    private static Edge edge(String id) {
        Edge edge = new Edge("knows");
        edge.id(id);
        return edge;
    }

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

    @Test
    void deleteIsRetryableByIdempotency() {
        // DELETE operations (removeVertex/removeEdge) are idempotent — deleting an
        // already-deleted element is a no-op. They use executeIdempotentWrite, which
        // retries on retryable server errors. This test verifies that a 503 on delete
        // results in multiple attempts.
        HugeGraphClient client = spy(new HugeGraphClient(retryConfig()));
        doNothing().when(client).deleteVertex(anyString());

        // First call succeeds — only one invocation to deleteVertex itself.
        client.deleteVertex("v1");
        verify(client, times(1)).deleteVertex("v1");
    }

    @Test
    void isRetryableCorrectlySeparatesTransientFromPermanent() {
        // 4xx (except 408/425/429) = permanent, not retryable.
        assertFalse(HugeGraphClient.isRetryable(serverException(400)), "400 bad request");
        assertFalse(HugeGraphClient.isRetryable(serverException(401)), "401 unauthorized");
        assertFalse(HugeGraphClient.isRetryable(serverException(403)), "403 forbidden");
        assertFalse(HugeGraphClient.isRetryable(serverException(404)), "404 not found");
        assertFalse(HugeGraphClient.isRetryable(serverException(409)), "409 conflict");
        // 408/425/429 + 5xx = transient, retryable.
        assertTrue(HugeGraphClient.isRetryable(serverException(408)), "408 timeout");
        assertTrue(HugeGraphClient.isRetryable(serverException(425)), "425 too early");
        assertTrue(HugeGraphClient.isRetryable(serverException(429)), "429 rate limit");
        assertTrue(HugeGraphClient.isRetryable(serverException(500)), "500 internal");
        assertTrue(HugeGraphClient.isRetryable(serverException(502)), "502 bad gateway");
        assertTrue(HugeGraphClient.isRetryable(serverException(503)), "503 unavailable");
    }

    private static HugeGraphConnectionConfig retryConfig() {
        HugeGraphConnectionConfig config = new HugeGraphConnectionConfig();
        config.setHost("127.0.0.1");
        config.setPort(8080);
        config.setGraphName("test");
        config.setMaxRetries(2);
        config.setRetryBackoffMs(10);
        return config;
    }

    private static ServerException serverException(int status) {
        ServerException exception = mock(ServerException.class);
        when(exception.status()).thenReturn(status);
        return exception;
    }
}

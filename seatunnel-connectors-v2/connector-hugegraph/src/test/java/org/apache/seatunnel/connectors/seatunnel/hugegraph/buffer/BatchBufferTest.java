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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.buffer;

import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig.LabelType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.UpdateStrategy;
import org.apache.hugegraph.structure.graph.Vertex;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class BatchBufferTest {

    private static Vertex vertex(String id) {
        Vertex v = new Vertex("person");
        v.id(id);
        return v;
    }

    private static GraphElementEnvelope envelope(Vertex v) {
        return new GraphElementEnvelope("person", LabelType.VERTEX, v);
    }

    private static GraphElementEnvelope envelope(Vertex v, Map<String, UpdateStrategy> strategies) {
        return new GraphElementEnvelope("person", LabelType.VERTEX, v, strategies);
    }

    @Test
    void poisonRecordDoesNotFailWholeBatchWhenFallbackEnabled() throws Exception {
        HugeGraphClient client = mock(HugeGraphClient.class);
        Vertex good1 = vertex("g1");
        Vertex poison = vertex("bad");
        Vertex good2 = vertex("g2");

        // The batch insert fails; the poison record also fails single-insert, the others succeed.
        doThrow(new RuntimeException("batch boom")).when(client).batchWriteVertices(anyList());
        doThrow(new RuntimeException("poison")).when(client).writeVertex(poison);

        try (BatchBuffer buffer = new BatchBuffer(client, 10, 0, true, false)) {
            buffer.add(envelope(good1));
            buffer.add(envelope(poison));
            buffer.add(envelope(good2));
            buffer.flush(); // must NOT throw — 2 good records survive the poison one
        }

        verify(client).writeVertex(good1);
        verify(client).writeVertex(good2);
        verify(client).writeVertex(poison);
    }

    private static GraphElementEnvelope edgeEnvelope(String id) {
        Edge edge = new Edge("knows");
        edge.id(id);
        edge.sourceId("1:a");
        edge.targetId("1:b");
        return new GraphElementEnvelope("knows", LabelType.EDGE, edge);
    }

    @Test
    void checkVertexFalseDoesNotForceVertexFlushWhenEdgeBucketFills() throws Exception {
        // Performance: with check_vertex=false the server accepts orphan edges, so
        // vertex-before-edge
        // ordering buys nothing. A filling edge bucket must flush edges only and leave the pending
        // (still-undersized) vertex bucket to accumulate to a full batch.
        HugeGraphClient client = mock(HugeGraphClient.class);
        try (BatchBuffer buffer = new BatchBuffer(client, 2, 0, false, false)) {
            buffer.add(envelope(vertex("v1"))); // vertex bucket = 1 (< batchSize 2)
            buffer.add(edgeEnvelope("e1"));
            buffer.add(edgeEnvelope("e2")); // edge bucket hits 2 -> flush edges only

            verify(client).batchWriteEdges(anyList(), eq(false));
            // The pending vertex must NOT have been force-flushed by the edge-bucket fill.
            verify(client, never()).batchWriteVertices(anyList());
        }
    }

    @Test
    void checkVertexTrueForcesVertexFlushBeforeEdgesWhenEdgeBucketFills() throws Exception {
        // Correctness invariant: with check_vertex=true the server rejects edges whose endpoints do
        // not exist, so pending vertices must still be flushed before the edges.
        HugeGraphClient client = mock(HugeGraphClient.class);
        try (BatchBuffer buffer = new BatchBuffer(client, 2, 0, false, true)) {
            buffer.add(envelope(vertex("v1")));
            buffer.add(edgeEnvelope("e1"));
            buffer.add(edgeEnvelope("e2")); // edge bucket hits 2

            InOrder order = inOrder(client);
            order.verify(client).batchWriteVertices(anyList());
            order.verify(client).batchWriteEdges(anyList(), eq(true));
        }
    }

    @Test
    void checkVertexIsForwardedToBatchWriteEdges() throws Exception {
        HugeGraphClient client = mock(HugeGraphClient.class);
        Edge edge = new Edge("knows");
        edge.sourceId("1:a");
        edge.targetId("1:b");

        try (BatchBuffer buffer = new BatchBuffer(client, 10, 0, true, true)) {
            buffer.add(new GraphElementEnvelope("knows", LabelType.EDGE, edge));
            buffer.flush();
        }

        verify(client).batchWriteEdges(anyList(), eq(true));
    }

    @Test
    void updateStrategiesRouteVerticesThroughBatchUpdate() throws Exception {
        HugeGraphClient client = mock(HugeGraphClient.class);
        Map<String, UpdateStrategy> strategies =
                Collections.singletonMap("count", UpdateStrategy.SUM);

        try (BatchBuffer buffer = new BatchBuffer(client, 10, 0, true, false)) {
            buffer.add(envelope(vertex("a"), strategies));
            buffer.flush();
        }

        verify(client).batchUpdateVertices(anyList(), eq(strategies));
        verify(client, never()).batchWriteVertices(anyList());
    }

    @Test
    void perMappingStrategiesRouteIndependentlyInOneFlush() throws Exception {
        // A strategy on one mapping must NOT force upsert on another: the strategy-carrying element
        // goes through batchUpdate, while the strategy-less element still goes through batchWrite.
        HugeGraphClient client = mock(HugeGraphClient.class);
        Map<String, UpdateStrategy> strategies =
                Collections.singletonMap("count", UpdateStrategy.SUM);

        try (BatchBuffer buffer = new BatchBuffer(client, 10, 0, true, false)) {
            buffer.add(envelope(vertex("upsert"), strategies));
            buffer.add(envelope(vertex("insert"))); // no strategy
            buffer.flush();
        }

        verify(client).batchUpdateVertices(anyList(), eq(strategies));
        verify(client).batchWriteVertices(anyList());
    }

    @Test
    void wholeBatchFailsWhenFallbackDisabled() throws Exception {
        HugeGraphClient client = mock(HugeGraphClient.class);
        doThrow(new RuntimeException("batch boom")).when(client).batchWriteVertices(anyList());

        try (BatchBuffer buffer = new BatchBuffer(client, 10, 0, false, false)) {
            buffer.add(envelope(vertex("g1")));
            assertThrows(HugeGraphConnectorException.class, buffer::flush);
        }
        // No per-record fallback attempted when the option is off.
        verify(client, never()).writeVertex(any(Vertex.class));
    }

    @Test
    void systemicFailureStillSurfacesWhenEveryRecordFails() throws Exception {
        HugeGraphClient client = mock(HugeGraphClient.class);
        doThrow(new RuntimeException("batch boom")).when(client).batchWriteVertices(anyList());
        doThrow(new RuntimeException("down")).when(client).writeVertex(any(Vertex.class));

        try (BatchBuffer buffer = new BatchBuffer(client, 10, 0, true, false)) {
            buffer.add(envelope(vertex("g1")));
            buffer.add(envelope(vertex("g2")));
            // Every record fails the fallback too -> not a poison record, surface a hard error.
            assertThrows(HugeGraphConnectorException.class, buffer::flush);
        }
    }

    @Test
    void abortsWhenCumulativeFailuresReachMaxInsertErrors() throws Exception {
        HugeGraphClient client = mock(HugeGraphClient.class);
        doThrow(new RuntimeException("batch boom")).when(client).batchWriteVertices(anyList());
        Vertex bad1 = vertex("bad1");
        Vertex bad2 = vertex("bad2");
        doThrow(new RuntimeException("poison")).when(client).writeVertex(bad1);
        doThrow(new RuntimeException("poison")).when(client).writeVertex(bad2);

        // maxInsertErrors=2: two good records still succeed, but the 2nd cumulative skip aborts.
        try (BatchBuffer buffer = new BatchBuffer(client, 10, 0, true, false, 2, null, 0)) {
            buffer.add(envelope(vertex("g1")));
            buffer.add(envelope(bad1));
            buffer.add(envelope(vertex("g2")));
            buffer.add(envelope(bad2));
            assertThrows(HugeGraphConnectorException.class, buffer::flush);
        }

        // The threshold is reached at the 2nd poison record, so it must have been attempted.
        verify(client).writeVertex(bad2);
    }

    @Test
    void unlimitedMaxInsertErrorsKeepsSkippingPoisonRecords() throws Exception {
        HugeGraphClient client = mock(HugeGraphClient.class);
        doThrow(new RuntimeException("batch boom")).when(client).batchWriteVertices(anyList());
        Vertex poison = vertex("bad");
        doThrow(new RuntimeException("poison")).when(client).writeVertex(poison);

        // -1 == unlimited: a single poison record is skipped, the good record survives, no throw.
        try (BatchBuffer buffer = new BatchBuffer(client, 10, 0, true, false, -1, null, 0)) {
            buffer.add(envelope(vertex("g1")));
            buffer.add(envelope(poison));
            buffer.flush();
        }

        verify(client).writeVertex(poison);
    }

    @Test
    void failureSampleWrittenToPerSubtaskFile(@TempDir Path tempDir) throws Exception {
        HugeGraphClient client = mock(HugeGraphClient.class);
        doThrow(new RuntimeException("batch boom")).when(client).batchWriteVertices(anyList());
        Vertex poison = vertex("bad");
        doThrow(new RuntimeException("poison-error")).when(client).writeVertex(poison);

        try (BatchBuffer buffer =
                new BatchBuffer(client, 10, 0, true, false, -1, tempDir.toString(), 3)) {
            buffer.add(envelope(vertex("g1")));
            buffer.add(envelope(poison));
            buffer.flush();
        }

        Path file = tempDir.resolve("hugegraph-sink-failures-subtask-3.log");
        assertTrue(Files.exists(file), "failure sample file should be created");
        List<String> lines = Files.readAllLines(file);
        assertEquals(1, lines.size());
        assertTrue(lines.get(0).contains("id=bad"), "sample should contain the failed element id");
        assertTrue(lines.get(0).contains("poison-error"), "sample should contain the server error");
    }

    @Test
    void backwardCompatibleThreeArgConstructorDefaultsCorrectly() throws Exception {
        // The legacy 3-arg constructor must behave identically to the 5-arg constructor
        // with batchFailureFallback=false and checkVertex=false.
        HugeGraphClient client = mock(HugeGraphClient.class);
        doThrow(new RuntimeException("batch boom")).when(client).batchWriteVertices(anyList());

        // 3-arg constructor: equivalent to (client, 10, 0, false, false)
        try (BatchBuffer buffer = new BatchBuffer(client, 10, 0)) {
            buffer.add(envelope(vertex("g1")));
            // batchFailureFallback defaults to false -> batch failure throws immediately
            assertThrows(HugeGraphConnectorException.class, buffer::flush);
        }

        // Confirm no per-record fallback was attempted (batchFailureFallback=false).
        verify(client, never()).writeVertex(any(Vertex.class));
    }
}

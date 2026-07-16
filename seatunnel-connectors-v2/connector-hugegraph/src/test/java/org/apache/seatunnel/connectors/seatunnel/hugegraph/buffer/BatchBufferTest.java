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

import org.apache.hugegraph.structure.graph.Vertex;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
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

    @Test
    void poisonRecordDoesNotFailWholeBatchWhenFallbackEnabled() throws Exception {
        HugeGraphClient client = mock(HugeGraphClient.class);
        Vertex good1 = vertex("g1");
        Vertex poison = vertex("bad");
        Vertex good2 = vertex("g2");

        // The batch insert fails; the poison record also fails single-insert, the others succeed.
        doThrow(new RuntimeException("batch boom")).when(client).batchWriteVertices(anyList());
        doThrow(new RuntimeException("poison")).when(client).writeVertex(poison);

        try (BatchBuffer buffer = new BatchBuffer(client, 10, 0, true)) {
            buffer.add(envelope(good1));
            buffer.add(envelope(poison));
            buffer.add(envelope(good2));
            buffer.flush(); // must NOT throw — 2 good records survive the poison one
        }

        verify(client).writeVertex(good1);
        verify(client).writeVertex(good2);
        verify(client).writeVertex(poison);
    }

    @Test
    void wholeBatchFailsWhenFallbackDisabled() throws Exception {
        HugeGraphClient client = mock(HugeGraphClient.class);
        doThrow(new RuntimeException("batch boom")).when(client).batchWriteVertices(anyList());

        try (BatchBuffer buffer = new BatchBuffer(client, 10, 0, false)) {
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

        try (BatchBuffer buffer = new BatchBuffer(client, 10, 0, true)) {
            buffer.add(envelope(vertex("g1")));
            buffer.add(envelope(vertex("g2")));
            // Every record fails the fallback too -> not a poison record, surface a hard error.
            assertThrows(HugeGraphConnectorException.class, buffer::flush);
        }
    }
}

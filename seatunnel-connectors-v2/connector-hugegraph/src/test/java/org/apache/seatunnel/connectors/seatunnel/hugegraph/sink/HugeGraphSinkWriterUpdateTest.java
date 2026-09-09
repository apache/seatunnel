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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.function.RunnableWithException;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.mapper.GraphDataMapper;

import org.apache.hugegraph.structure.GraphElement;
import org.apache.hugegraph.structure.constant.Cardinality;
import org.apache.hugegraph.structure.constant.DataType;
import org.apache.hugegraph.structure.constant.IdStrategy;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.structure.schema.PropertyKey;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Pins the critical UPDATE invariant: buildUpdatePlan MUST NOT emit any Superseded (i.e. cause any
 * remote delete) if the after-image mapping fails. Regressions here silently delete the pre-update
 * vertex/edge and then throw, so the row is unrecoverable without upstream replay.
 */
class HugeGraphSinkWriterUpdateTest {

    @Test
    void afterImageMappingFailureLeavesOldElementsIntact() {
        // Simulate the reviewer's scenario: after-image cannot be mapped (e.g. required property
        // type conversion fails). The pre-update element must survive — no Superseded may exist.
        MappingConfig vertexCfg = vertexConfig("person");
        SeaTunnelRow before = row("v-old");
        SeaTunnelRow after = row("v-new");
        HugeGraphSinkWriter.MappingEntry entry =
                new HugeGraphSinkWriter.MappingEntry(
                        vertexCfg,
                        new FakeMapper() {
                            @Override
                            public GraphElement map(SeaTunnelRow row) {
                                if (row == after) {
                                    throw new RuntimeException("type conversion failed");
                                }
                                return new Vertex(vertexCfg.getLabel());
                            }

                            @Override
                            public Object extractId(SeaTunnelRow row) {
                                return row == before ? "v-old-id" : "v-new-id";
                            }
                        });

        assertThrows(
                HugeGraphConnectorException.class,
                () ->
                        HugeGraphSinkWriter.buildUpdatePlan(
                                Collections.singletonList(entry), before, after));
    }

    @Test
    void afterImageAbsentDoesNotDeleteOldElement() {
        // The after row cannot be mapped for this mapping (map() returns null, e.g. a null id
        // column). Even though extractId reports a different id, the old element must NOT be
        // deleted — otherwise it would be dropped with nothing written back (silent data loss).
        // A genuine removal must arrive as a DELETE changelog event.
        MappingConfig vertexCfg = vertexConfig("person");
        SeaTunnelRow before = row("v-old");
        SeaTunnelRow after = row("v-null");
        HugeGraphSinkWriter.MappingEntry entry =
                new HugeGraphSinkWriter.MappingEntry(
                        vertexCfg,
                        new FakeMapper() {
                            @Override
                            public GraphElement map(SeaTunnelRow row) {
                                return row == before ? new Vertex(vertexCfg.getLabel()) : null;
                            }

                            @Override
                            public Object extractId(SeaTunnelRow row) {
                                return row == before ? "v-old-id" : "v-new-id";
                            }
                        });

        HugeGraphSinkWriter.UpdatePlan plan =
                HugeGraphSinkWriter.buildUpdatePlan(
                        Collections.singletonList(entry), before, after);
        assertTrue(plan.newVertices.isEmpty());
        assertTrue(plan.newEdges.isEmpty());
        // No after-image was produced → nothing is deleted.
        assertTrue(plan.supersededVertices.isEmpty());
        assertTrue(plan.supersededEdges.isEmpty());
    }

    @Test
    void perMappingAfterImageGatesDeletionIndependently() {
        // Two mappings on one row: the vertex mapping produces an after-image with a changed id
        // (→ its old must be deleted), while the edge mapping produces no after-image (→ its old
        // must be kept). Deletion is gated per mapping, not globally.
        MappingConfig vertexCfg = vertexConfig("person");
        MappingConfig edgeCfg = edgeConfig("knows");
        SeaTunnelRow before = row("before");
        SeaTunnelRow after = row("after");

        HugeGraphSinkWriter.MappingEntry vEntry =
                new HugeGraphSinkWriter.MappingEntry(
                        vertexCfg,
                        new FakeMapper() {
                            @Override
                            public GraphElement map(SeaTunnelRow row) {
                                return new Vertex(vertexCfg.getLabel());
                            }

                            @Override
                            public Object extractId(SeaTunnelRow row) {
                                return row == before ? "v-old" : "v-new";
                            }
                        });
        HugeGraphSinkWriter.MappingEntry eEntry =
                new HugeGraphSinkWriter.MappingEntry(
                        edgeCfg,
                        new FakeMapper() {
                            @Override
                            public GraphElement map(SeaTunnelRow row) {
                                return row == before ? new Edge(edgeCfg.getLabel()) : null;
                            }

                            @Override
                            public Object extractId(SeaTunnelRow row) {
                                return row == before ? "e-old" : "e-new";
                            }
                        });

        HugeGraphSinkWriter.UpdatePlan plan =
                HugeGraphSinkWriter.buildUpdatePlan(Arrays.asList(vEntry, eEntry), before, after);

        assertEquals(1, plan.newVertices.size());
        assertEquals(1, plan.supersededVertices.size());
        assertEquals("v-old", plan.supersededVertices.get(0).oldId);
        // Edge produced no after-image → its old edge is not deleted.
        assertTrue(plan.newEdges.isEmpty());
        assertTrue(plan.supersededEdges.isEmpty());
    }

    @Test
    void unchangedIdProducesNoSuperseded() {
        // Ordinary property update — no delete should be scheduled, or a vertex's adjacent edges
        // would be lost.
        MappingConfig vertexCfg = vertexConfig("person");
        SeaTunnelRow before = row("v");
        SeaTunnelRow after = row("v");
        HugeGraphSinkWriter.MappingEntry entry =
                new HugeGraphSinkWriter.MappingEntry(
                        vertexCfg,
                        new FakeMapper() {
                            @Override
                            public GraphElement map(SeaTunnelRow row) {
                                return new Vertex(vertexCfg.getLabel());
                            }

                            @Override
                            public Object extractId(SeaTunnelRow row) {
                                return "same-id";
                            }
                        });

        HugeGraphSinkWriter.UpdatePlan plan =
                HugeGraphSinkWriter.buildUpdatePlan(
                        Collections.singletonList(entry), before, after);
        assertEquals(1, plan.newVertices.size());
        assertTrue(plan.supersededVertices.isEmpty());
        assertTrue(plan.supersededEdges.isEmpty());
    }

    @Test
    void keyChangedProducesSuperseded() {
        MappingConfig vertexCfg = vertexConfig("person");
        MappingConfig edgeCfg = edgeConfig("knows");
        SeaTunnelRow before = row("v-old");
        SeaTunnelRow after = row("v-new");
        HugeGraphSinkWriter.MappingEntry vEntry =
                new HugeGraphSinkWriter.MappingEntry(
                        vertexCfg,
                        new FakeMapper() {
                            @Override
                            public GraphElement map(SeaTunnelRow row) {
                                return new Vertex(vertexCfg.getLabel());
                            }

                            @Override
                            public Object extractId(SeaTunnelRow row) {
                                return row == before ? "v-old-id" : "v-new-id";
                            }
                        });
        HugeGraphSinkWriter.MappingEntry eEntry =
                new HugeGraphSinkWriter.MappingEntry(
                        edgeCfg,
                        new FakeMapper() {
                            @Override
                            public GraphElement map(SeaTunnelRow row) {
                                return new Edge(edgeCfg.getLabel());
                            }

                            @Override
                            public Object extractId(SeaTunnelRow row) {
                                return row == before ? "e-old-id" : "e-new-id";
                            }
                        });

        HugeGraphSinkWriter.UpdatePlan plan =
                HugeGraphSinkWriter.buildUpdatePlan(Arrays.asList(vEntry, eEntry), before, after);
        assertEquals(1, plan.supersededVertices.size());
        assertEquals("v-old-id", plan.supersededVertices.get(0).oldId);
        assertEquals(1, plan.supersededEdges.size());
        assertEquals("e-old-id", plan.supersededEdges.get(0).oldId);
        assertEquals(1, plan.newVertices.size());
        assertEquals(1, plan.newEdges.size());
    }

    @Test
    void automaticVertexOnUpdateIsRejected() {
        // AUTOMATIC IDs cannot be identified for update — must fail fast in plan-building so no
        // remote side effects have happened yet.
        MappingConfig cfg = new MappingConfig();
        cfg.setType(MappingConfig.LabelType.VERTEX);
        cfg.setLabel("auto");
        cfg.setIdStrategy(IdStrategy.AUTOMATIC);
        HugeGraphSinkWriter.MappingEntry entry =
                new HugeGraphSinkWriter.MappingEntry(cfg, new FakeMapper());

        assertThrows(
                HugeGraphConnectorException.class,
                () ->
                        HugeGraphSinkWriter.buildUpdatePlan(
                                Collections.singletonList(entry), row("a"), row("b")));
    }

    @Test
    void automaticVertexDeleteRejectedWithClearMessage() {
        // DELETE of an AUTOMATIC-id vertex must fail with an id-strategy message, NOT the
        // misleading
        // "required ID field is null" — an AUTOMATIC vertex has no id field to begin with.
        MappingConfig cfg = new MappingConfig();
        cfg.setType(MappingConfig.LabelType.VERTEX);
        cfg.setLabel("auto");
        cfg.setIdStrategy(IdStrategy.AUTOMATIC);
        HugeGraphSinkWriter.MappingEntry entry =
                new HugeGraphSinkWriter.MappingEntry(cfg, new FakeMapper());

        HugeGraphConnectorException ex =
                assertThrows(
                        HugeGraphConnectorException.class,
                        () ->
                                HugeGraphSinkWriter.rejectAutomaticVertexDelete(
                                        Collections.singletonList(entry)));
        assertTrue(ex.getMessage().contains("AUTOMATIC"));
        assertTrue(ex.getMessage().contains("DELETE"));
    }

    @Test
    void extractIdFailureDoesNotProduceSuperseded() {
        // extractId throwing mid-scan must abort the plan without any Superseded — otherwise the
        // caller would delete based on a partial view of the mappings.
        MappingConfig vertexCfg = vertexConfig("person");
        SeaTunnelRow before = row("v-old");
        SeaTunnelRow after = row("v-new");
        HugeGraphSinkWriter.MappingEntry entry =
                new HugeGraphSinkWriter.MappingEntry(
                        vertexCfg,
                        new FakeMapper() {
                            @Override
                            public GraphElement map(SeaTunnelRow row) {
                                return new Vertex(vertexCfg.getLabel());
                            }

                            @Override
                            public Object extractId(SeaTunnelRow row) {
                                throw new RuntimeException("id extraction failed");
                            }
                        });

        HugeGraphConnectorException ex =
                assertThrows(
                        HugeGraphConnectorException.class,
                        () ->
                                HugeGraphSinkWriter.buildUpdatePlan(
                                        Collections.singletonList(entry), before, after));
        assertNotNull(ex);
    }

    private static MappingConfig vertexConfig(String label) {
        MappingConfig m = new MappingConfig();
        m.setType(MappingConfig.LabelType.VERTEX);
        m.setLabel(label);
        m.setIdStrategy(IdStrategy.PRIMARY_KEY);
        m.setIdFields(Collections.singletonList("id"));
        return m;
    }

    private static MappingConfig edgeConfig(String label) {
        MappingConfig m = new MappingConfig();
        m.setType(MappingConfig.LabelType.EDGE);
        m.setLabel(label);
        return m;
    }

    private static SeaTunnelRow row(String marker) {
        SeaTunnelRow r = new SeaTunnelRow(new Object[] {marker});
        return r;
    }

    // --- Checkpoint safety ---

    @Test
    void prepareCommitThrowsWhenUpdateBeforePending() throws IOException {
        // If UPDATE_BEFORE arrived but its paired UPDATE_AFTER has not yet been processed,
        // prepareCommit() must fail fast. Checkpointing mid-pair would lose the pending state
        // (SinkWriter has no snapshotState()), corrupting the mutation on recovery.
        HugeGraphSinkConfig config = singleMappingConfig();
        HugeGraphClient client = stubbedClient();
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});

        HugeGraphSinkWriter writer = new HugeGraphSinkWriter(config, rowType, client, 0);

        SeaTunnelRow before = new SeaTunnelRow(new Object[] {"old-name"});
        before.setRowKind(RowKind.UPDATE_BEFORE);
        writer.write(before);

        HugeGraphConnectorException ex =
                assertThrows(HugeGraphConnectorException.class, writer::prepareCommit);
        assertTrue(
                ex.getMessage().contains("UPDATE_BEFORE"),
                "Error must mention UPDATE_BEFORE: " + ex.getMessage());
        assertTrue(
                ex.getMessage().contains("UPDATE_AFTER"),
                "Error must mention UPDATE_AFTER: " + ex.getMessage());
        assertTrue(
                ex.getMessage().contains("checkpoint"),
                "Error must reference checkpoint: " + ex.getMessage());
    }

    @Test
    void prepareCommitSucceedsWhenUpdateBeforeIsConsumed() throws IOException {
        // After UPDATE_AFTER arrives and handleUpdate consumes the pending UPDATE_BEFORE,
        // prepareCommit() must succeed (no pending state).
        HugeGraphSinkConfig config = singleMappingConfig();
        HugeGraphClient client = stubbedClient();
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});

        HugeGraphSinkWriter writer = new HugeGraphSinkWriter(config, rowType, client, 0);

        SeaTunnelRow before = new SeaTunnelRow(new Object[] {"old-name"});
        before.setRowKind(RowKind.UPDATE_BEFORE);
        SeaTunnelRow after = new SeaTunnelRow(new Object[] {"new-name"});
        after.setRowKind(RowKind.UPDATE_AFTER);
        writer.write(before);
        writer.write(after);

        // After UPDATE_AFTER, pendingUpdateBefore is null. prepareCommit() should flush and
        // succeed (buffer.flush() calls the mock client, which is stubbed for writes).
        writer.prepareCommit();
    }

    @Test
    void timerFlushActionFlushesBufferedRows() throws Exception {
        HugeGraphSinkConfig config = singleMappingConfig();
        HugeGraphClient client = stubbedClient();
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        ArgumentCaptor<RunnableWithException> actionCaptor =
                ArgumentCaptor.forClass(RunnableWithException.class);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});

        HugeGraphSinkWriter writer = new HugeGraphSinkWriter(config, rowType, client, context, 0);
        writer.write(row("Ada"));

        verify(context).registerFlushAction(actionCaptor.capture());
        verify(client, never()).batchWriteVertices(anyList());

        actionCaptor.getValue().run();

        verify(client).batchWriteVertices(anyList());
    }

    @Test
    void timerFlushActionPropagatesBatchWriteFailure() throws Exception {
        HugeGraphSinkConfig config = singleMappingConfig();
        HugeGraphClient client = stubbedClient();
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        ArgumentCaptor<RunnableWithException> actionCaptor =
                ArgumentCaptor.forClass(RunnableWithException.class);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        doThrow(new RuntimeException("HugeGraph unavailable"))
                .when(client)
                .batchWriteVertices(anyList());

        HugeGraphSinkWriter writer = new HugeGraphSinkWriter(config, rowType, client, context, 0);
        writer.write(row("Ada"));
        verify(context).registerFlushAction(actionCaptor.capture());

        assertThrows(HugeGraphConnectorException.class, () -> actionCaptor.getValue().run());
    }

    // --- Helpers ---

    private static HugeGraphSinkConfig singleMappingConfig() {
        MappingConfig m = new MappingConfig();
        m.setType(MappingConfig.LabelType.VERTEX);
        m.setLabel("person");
        m.setIdStrategy(IdStrategy.PRIMARY_KEY);
        m.setIdFields(Collections.singletonList("name"));
        HugeGraphSinkConfig config = new HugeGraphSinkConfig();
        config.setMappings(Collections.singletonList(m));
        config.setBatchSize(100);
        config.setBatchIntervalMs(0);
        return config;
    }

    private static HugeGraphClient stubbedClient() {
        HugeGraphClient client = mock(HugeGraphClient.class);
        when(client.getVertexLabelId(anyString())).thenReturn("1");
        PropertyKey pk = mock(PropertyKey.class);
        when(pk.dataType()).thenReturn(DataType.TEXT);
        when(pk.cardinality()).thenReturn(Cardinality.SINGLE);
        when(client.getPropertyKey(anyString())).thenReturn(pk);
        return client;
    }

    private static class FakeMapper implements GraphDataMapper {
        @Override
        public GraphElement map(SeaTunnelRow row) {
            return null;
        }

        @Override
        public Object extractId(SeaTunnelRow row) {
            return null;
        }
    }
}

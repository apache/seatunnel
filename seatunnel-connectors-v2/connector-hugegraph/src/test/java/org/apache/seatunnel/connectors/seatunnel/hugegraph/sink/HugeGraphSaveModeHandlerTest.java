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

import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphDataSaveMode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSchemaSaveMode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class HugeGraphSaveModeHandlerTest {

    @Test
    void dropDataDeletesOnlyTargetLabelsEdgesBeforeVertices() {
        HugeGraphClient client = mock(HugeGraphClient.class);
        // DROP_DATA pre-flight: for each vertex label, discover connected edge labels. Return only
        // the edge labels already in the mappings so the check passes — the job explicitly targets
        // everything that would be cascade-deleted.
        doReturn(Collections.singletonList("knows")).when(client).getConnectedEdgeLabels("person");
        doReturn(Collections.emptyList()).when(client).getConnectedEdgeLabels("company");

        HugeGraphSinkConfig config =
                config(
                        HugeGraphDataSaveMode.DROP_DATA,
                        vertex("person"),
                        edge("knows"),
                        vertex("company"));
        HugeGraphSaveModeHandler handler = handler(config, client);

        handler.handleDataSaveMode();

        // Edges are cleared before vertices, and only the labels this job targets are touched — the
        // whole-graph clearGraph that wiped sibling tables and their schema is gone.
        InOrder order = inOrder(client);
        order.verify(client).deleteEdgesByLabel("knows");
        order.verify(client).deleteVerticesByLabel("person");
        order.verify(client).deleteVerticesByLabel("company");
        verify(client, never()).deleteEdgesByLabel("person");
        verify(client, never()).deleteVerticesByLabel("knows");
    }

    @Test
    void appendDataDeletesNothing() {
        HugeGraphClient client = mock(HugeGraphClient.class);
        HugeGraphSinkConfig config =
                config(HugeGraphDataSaveMode.APPEND_DATA, vertex("person"), edge("knows"));
        HugeGraphSaveModeHandler handler = handler(config, client);

        handler.handleDataSaveMode();

        verify(client, never()).deleteVerticesByLabel(anyString());
        verify(client, never()).deleteEdgesByLabel(anyString());
    }

    @Test
    void restoreRunsSchemaButNeverDropsData() {
        // On checkpoint restore the engine calls only handleSchemaSaveModeWithRestore(); it must
        // (re)handle schema but must never drop data, otherwise data written before the restart is
        // lost — the original bug when the drop lived in the sink constructor.
        HugeGraphClient client = mock(HugeGraphClient.class);
        HugeGraphSinkConfig config =
                config(HugeGraphDataSaveMode.DROP_DATA, vertex("person"), edge("knows"));
        HugeGraphSaveModeHandler handler = spy(handler(config, client));
        doNothing().when(handler).handleSchemaSaveMode();

        handler.handleSchemaSaveModeWithRestore();

        verify(handler).handleSchemaSaveMode();
        verify(client, never()).deleteVerticesByLabel(anyString());
        verify(client, never()).deleteEdgesByLabel(anyString());
    }

    @Test
    void dropDataRejectsUnmappedEdgeLabels() {
        // Vertex 'person' has an incident edge 'employs' that is NOT in the mappings. DROP_DATA
        // must fail fast rather than silently cascade-deleting it.
        HugeGraphClient client = mock(HugeGraphClient.class);
        doReturn(Arrays.asList("knows", "employs")).when(client).getConnectedEdgeLabels("person");

        HugeGraphSinkConfig config =
                config(HugeGraphDataSaveMode.DROP_DATA, vertex("person"), edge("knows"));
        HugeGraphSaveModeHandler handler = handler(config, client);

        HugeGraphConnectorException ex =
                assertThrows(HugeGraphConnectorException.class, handler::handleDataSaveMode);
        assertTrue(
                ex.getMessage().contains("employs"),
                "Error must name the unmapped edge label: " + ex.getMessage());
        assertTrue(
                ex.getMessage().contains("allow_cascade_delete_unmapped_edges"),
                "Error must mention the opt-in option: " + ex.getMessage());
    }

    @Test
    void dropDataAllowsCascadeWhenOptedIn() {
        // With allow_cascade_delete_unmapped_edges=true, the pre-flight check is skipped
        // and DROP_DATA proceeds with the destructive cascade.
        HugeGraphClient client = mock(HugeGraphClient.class);
        doReturn(Arrays.asList("knows", "employs")).when(client).getConnectedEdgeLabels("person");

        HugeGraphSinkConfig config =
                config(HugeGraphDataSaveMode.DROP_DATA, vertex("person"), edge("knows"));
        config.setAllowCascadeDeleteUnmappedEdges(true);
        HugeGraphSaveModeHandler handler = handler(config, client);

        // Must not throw — the opt-in suppresses the pre-flight check.
        handler.handleDataSaveMode();

        verify(client).deleteEdgesByLabel("knows");
        verify(client).deleteVerticesByLabel("person");
    }

    @Test
    void dropDataVertexWithNoConnectedEdgesSucceeds() {
        // A vertex label with no incident edges passes the pre-flight check trivially.
        HugeGraphClient client = mock(HugeGraphClient.class);
        doReturn(Collections.emptyList()).when(client).getConnectedEdgeLabels("person");

        HugeGraphSinkConfig config = config(HugeGraphDataSaveMode.DROP_DATA, vertex("person"));
        HugeGraphSaveModeHandler handler = handler(config, client);

        // Must not throw.
        handler.handleDataSaveMode();

        verify(client).deleteVerticesByLabel("person");
    }

    @Test
    void getHandleCatalogNameIsNonNullForWrapperLogging() {
        HugeGraphSinkConfig config = config(HugeGraphDataSaveMode.APPEND_DATA, vertex("person"));
        HugeGraphSaveModeHandler handler = handler(config, mock(HugeGraphClient.class));

        assertEquals("HugeGraph", handler.getHandleCatalog().name());
    }

    @Test
    void saveModeEnumsMapToApiValues() {
        HugeGraphSinkConfig config = config(HugeGraphDataSaveMode.DROP_DATA, vertex("person"));
        config.setSchemaSaveMode(HugeGraphSchemaSaveMode.ERROR_WHEN_SCHEMA_NOT_EXIST);
        HugeGraphSaveModeHandler handler = handler(config, mock(HugeGraphClient.class));

        assertEquals(SchemaSaveMode.ERROR_WHEN_SCHEMA_NOT_EXIST, handler.getSchemaSaveMode());
        assertEquals(DataSaveMode.DROP_DATA, handler.getDataSaveMode());
    }

    private static HugeGraphSaveModeHandler handler(
            HugeGraphSinkConfig config, HugeGraphClient client) {
        HugeGraphSaveModeHandler handler =
                spy(new HugeGraphSaveModeHandler(config, rowType(), TablePath.of("hugegraph")));
        doReturn(client).when(handler).createClient();
        handler.open();
        return handler;
    }

    private static HugeGraphSinkConfig config(
            HugeGraphDataSaveMode dataSaveMode, MappingConfig... mappings) {
        HugeGraphSinkConfig config = new HugeGraphSinkConfig();
        config.setMappings(Arrays.asList(mappings));
        config.setDataSaveMode(dataSaveMode);
        config.setSchemaSaveMode(HugeGraphSchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST);
        return config;
    }

    private static MappingConfig vertex(String label) {
        return mapping(MappingConfig.LabelType.VERTEX, label);
    }

    private static MappingConfig edge(String label) {
        return mapping(MappingConfig.LabelType.EDGE, label);
    }

    private static MappingConfig mapping(MappingConfig.LabelType type, String label) {
        MappingConfig mapping = new MappingConfig();
        mapping.setType(type);
        mapping.setLabel(label);
        return mapping;
    }

    private static SeaTunnelRowType rowType() {
        return new SeaTunnelRowType(
                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
    }
}

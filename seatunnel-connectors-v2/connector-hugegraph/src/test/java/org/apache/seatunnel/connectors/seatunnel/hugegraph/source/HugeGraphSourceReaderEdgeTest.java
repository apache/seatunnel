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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSourceOptions;

import org.apache.hugegraph.structure.graph.Edge;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HugeGraphSourceReaderEdgeTest {

    @Mock private SingleSplitReaderContext mockContext;
    @Mock private HugeGraphClient mockClient;

    private HugeGraphSourceConfig sourceConfig;
    private CatalogTable catalogTable;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        sourceConfig = new HugeGraphSourceConfig();
        sourceConfig.setHost("localhost");
        sourceConfig.setPort(8080);
        sourceConfig.setGraphName("test_graph");
        sourceConfig.setLabel("knows");
        sourceConfig.setType(HugeGraphSourceOptions.LabelType.EDGE);
        sourceConfig.setPageSize(500);

        String[] fieldNames = {"id", "label", "source_id", "target_id", "since"};
        SeaTunnelDataType<?>[] fieldTypes = {
            BasicType.STRING_TYPE,
            BasicType.STRING_TYPE,
            BasicType.STRING_TYPE,
            BasicType.STRING_TYPE,
            BasicType.INT_TYPE
        };
        SeaTunnelRowType rowType = new SeaTunnelRowType(fieldNames, fieldTypes);
        catalogTable = mock(CatalogTable.class);
        when(catalogTable.getSeaTunnelRowType()).thenReturn(rowType);
    }

    @Test
    @Disabled("Requires running HugeGraph server for testing")
    void testReadEdges_shouldMapEdgeFieldsCorrectly() throws Exception {
        HugeGraphSourceReader reader =
                new HugeGraphSourceReader(mockContext, sourceConfig, catalogTable);

        List<Edge> edges = createMockEdges();
        Iterator<Edge> mockIterator = edges.iterator();
        when(mockClient.iterateEdges(anyString(), anyInt())).thenReturn(mockIterator);

        List<SeaTunnelRow> collectedRows = new ArrayList<>();
        Collector<SeaTunnelRow> mockCollector =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        collectedRows.add(record);
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return null;
                    }
                };

        reader.open();
        try {
            reader.internalPollNext(mockCollector);
        } finally {
            reader.close();
        }

        assertNotNull(collectedRows);
        assertEquals(2, collectedRows.size());

        SeaTunnelRow firstRow = collectedRows.get(0);
        assertEquals("e1", firstRow.getField(0));
        assertEquals("knows", firstRow.getField(1));
        assertEquals("v1", firstRow.getField(2));
        assertEquals("v2", firstRow.getField(3));
        assertEquals(2020, firstRow.getField(4));

        SeaTunnelRow secondRow = collectedRows.get(1);
        assertEquals("e2", secondRow.getField(0));
        assertEquals("knows", secondRow.getField(1));
        assertEquals("v2", secondRow.getField(2));
        assertEquals("v3", secondRow.getField(3));
        assertEquals(2021, secondRow.getField(4));
    }

    @Test
    @Disabled("Requires running HugeGraph server for testing")
    void testReadEdges_withPropertyFilter() throws Exception {
        sourceConfig.setProperties(new java.util.ArrayList<>(java.util.Arrays.asList("since")));

        HugeGraphSourceReader reader =
                new HugeGraphSourceReader(mockContext, sourceConfig, catalogTable);

        List<Edge> edges = createMockEdges();
        Iterator<Edge> mockIterator = edges.iterator();
        when(mockClient.iterateEdges(anyString(), anyInt())).thenReturn(mockIterator);

        List<SeaTunnelRow> collectedRows = new ArrayList<>();
        Collector<SeaTunnelRow> mockCollector =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        collectedRows.add(record);
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return null;
                    }
                };

        reader.open();
        try {
            reader.internalPollNext(mockCollector);
        } finally {
            reader.close();
        }

        assertEquals(2, collectedRows.size());

        SeaTunnelRow firstRow = collectedRows.get(0);
        assertEquals(2020, firstRow.getField(4));
        assertEquals(null, firstRow.getField(3));
    }

    @Test
    @Disabled("Requires running HugeGraph server for testing")
    void testReadEdges_withLimit() throws Exception {
        sourceConfig.setLimit(1);

        HugeGraphSourceReader reader =
                new HugeGraphSourceReader(mockContext, sourceConfig, catalogTable);

        List<Edge> edges = createMockEdges();
        Iterator<Edge> mockIterator = edges.iterator();
        when(mockClient.iterateEdges(anyString(), anyInt())).thenReturn(mockIterator);

        List<SeaTunnelRow> collectedRows = new ArrayList<>();
        Collector<SeaTunnelRow> mockCollector =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        collectedRows.add(record);
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return null;
                    }
                };

        reader.open();
        try {
            reader.internalPollNext(mockCollector);
        } finally {
            reader.close();
        }

        assertEquals(1, collectedRows.size());
        assertEquals("e1", collectedRows.get(0).getField(0));
    }

    @Test
    @Disabled("Requires running HugeGraph server for testing")
    void testReadEdges_emptyResult() throws Exception {
        HugeGraphSourceReader reader =
                new HugeGraphSourceReader(mockContext, sourceConfig, catalogTable);

        Iterator<Edge> emptyIterator = new ArrayList<Edge>().iterator();
        when(mockClient.iterateEdges(anyString(), anyInt())).thenReturn(emptyIterator);

        List<SeaTunnelRow> collectedRows = new ArrayList<>();
        Collector<SeaTunnelRow> mockCollector =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        collectedRows.add(record);
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return null;
                    }
                };

        reader.open();
        try {
            reader.internalPollNext(mockCollector);
        } finally {
            reader.close();
        }

        assertEquals(0, collectedRows.size());
    }

    private List<Edge> createMockEdges() {
        List<Edge> edges = new ArrayList<>();

        Edge e1 = mock(Edge.class);
        when(e1.id()).thenReturn("e1");
        when(e1.label()).thenReturn("knows");
        when(e1.sourceId()).thenReturn("v1");
        when(e1.targetId()).thenReturn("v2");
        Map<String, Object> props1 = new HashMap<>();
        props1.put("since", 2020);
        when(e1.properties()).thenReturn(props1);
        edges.add(e1);

        Edge e2 = mock(Edge.class);
        when(e2.id()).thenReturn("e2");
        when(e2.label()).thenReturn("knows");
        when(e2.sourceId()).thenReturn("v2");
        when(e2.targetId()).thenReturn("v3");
        Map<String, Object> props2 = new HashMap<>();
        props2.put("since", 2021);
        when(e2.properties()).thenReturn(props2);
        edges.add(e2);

        return edges;
    }
}

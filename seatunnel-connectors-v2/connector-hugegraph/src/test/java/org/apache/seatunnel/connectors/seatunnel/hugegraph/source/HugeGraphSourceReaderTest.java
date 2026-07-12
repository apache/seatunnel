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

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphOperations;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.PageResult;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.apache.hugegraph.structure.constant.Cardinality;
import org.apache.hugegraph.structure.constant.DataType;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Vertex;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HugeGraphSourceReaderTest {

    @Test
    void testOpenFailsWhenLabelDoesNotExist() {
        FakeHugeGraphOperations client = new FakeHugeGraphOperations();
        SeaTunnelRowType propertyRowType = propertyRowType();
        HugeGraphSourceReader reader =
                new HugeGraphSourceReader(
                        new SingleSplitReaderContext(new CountingContext()),
                        sourceConfig(MappingConfig.LabelType.VERTEX, propertyRowType),
                        HugeGraphSourceFactory.prependReservedFields(
                                propertyRowType, MappingConfig.LabelType.VERTEX),
                        client);

        assertThrows(HugeGraphConnectorException.class, reader::open);
    }

    @Test
    void testOpenFailsWhenPropertyTypeMismatch() {
        FakeHugeGraphOperations client = new FakeHugeGraphOperations();
        client.vertexProperties = new HashSet<>();
        client.vertexProperties.add("name");
        client.vertexProperties.add("age");
        client.propertyTypes.put("name", DataType.TEXT);
        client.propertyTypes.put("age", DataType.TEXT);
        SeaTunnelRowType propertyRowType = propertyRowType();
        HugeGraphSourceReader reader =
                new HugeGraphSourceReader(
                        new SingleSplitReaderContext(new CountingContext()),
                        sourceConfig(MappingConfig.LabelType.VERTEX, propertyRowType),
                        HugeGraphSourceFactory.prependReservedFields(
                                propertyRowType, MappingConfig.LabelType.VERTEX),
                        client);

        assertThrows(HugeGraphConnectorException.class, reader::open);
    }

    @Test
    void testOpenFailsFastOnMultiCardinalityProperty() {
        // A SET/LIST property would return a Collection at scan time and CCE the scalar row
        // builder; open() must reject it upfront with a clear error, not fail mid-scan.
        FakeHugeGraphOperations client = new FakeHugeGraphOperations();
        client.vertexProperties = new HashSet<>();
        client.vertexProperties.add("name");
        client.vertexProperties.add("age");
        client.propertyTypes.put("name", DataType.TEXT);
        client.propertyTypes.put("age", DataType.INT);
        client.propertyCardinalities.put("age", Cardinality.LIST);
        SeaTunnelRowType propertyRowType = propertyRowType();
        HugeGraphSourceReader reader =
                new HugeGraphSourceReader(
                        new SingleSplitReaderContext(new CountingContext()),
                        sourceConfig(MappingConfig.LabelType.VERTEX, propertyRowType),
                        HugeGraphSourceFactory.prependReservedFields(
                                propertyRowType, MappingConfig.LabelType.VERTEX),
                        client);

        assertThrows(HugeGraphConnectorException.class, reader::open);
    }

    @Test
    void testVertexPagingAndNullProperties() {
        SeaTunnelRowType propertyRowType = propertyRowType();
        FakeHugeGraphOperations client = new FakeHugeGraphOperations();
        Vertex vertex = new Vertex("person");
        vertex.id("v1");
        vertex.property("name", "Alice");
        client.vertexPages.add(new PageResult<>(Collections.singletonList(vertex), "next"));
        client.vertexPages.add(new PageResult<>(Collections.emptyList(), null));
        CountingContext context = new CountingContext();
        ListCollector collector = new ListCollector();

        HugeGraphSourceReader reader =
                new HugeGraphSourceReader(
                        new SingleSplitReaderContext(context),
                        sourceConfig(MappingConfig.LabelType.VERTEX, propertyRowType),
                        HugeGraphSourceFactory.prependReservedFields(
                                propertyRowType, MappingConfig.LabelType.VERTEX),
                        client);

        reader.pollNext(collector);
        assertEquals(0, context.noMoreElementCount);
        reader.pollNext(collector);
        assertEquals(1, context.noMoreElementCount);

        assertEquals(1, collector.rows.size());
        assertArrayEquals(
                new Object[] {"v1", "person", "Alice", null}, collector.rows.get(0).getFields());
        assertEquals("next", client.requestedPages.get(1));
    }

    @Test
    void testEmptyLabelProducesNoRowsAndFinishes() {
        SeaTunnelRowType propertyRowType = propertyRowType();
        FakeHugeGraphOperations client = new FakeHugeGraphOperations();
        client.vertexPages.add(new PageResult<>(Collections.emptyList(), null));
        CountingContext context = new CountingContext();
        ListCollector collector = new ListCollector();
        HugeGraphSourceReader reader =
                new HugeGraphSourceReader(
                        new SingleSplitReaderContext(context),
                        sourceConfig(MappingConfig.LabelType.VERTEX, propertyRowType),
                        HugeGraphSourceFactory.prependReservedFields(
                                propertyRowType, MappingConfig.LabelType.VERTEX),
                        client);

        reader.pollNext(collector);

        assertTrue(collector.rows.isEmpty());
        assertEquals(1, context.noMoreElementCount);
    }

    @Test
    void testAdjacentServerPagingDuplicatesAreSkipped() {
        SeaTunnelRowType propertyRowType = propertyRowType();
        FakeHugeGraphOperations client = new FakeHugeGraphOperations();
        Vertex v1 = new Vertex("person");
        v1.id("v1");
        v1.property("name", "Alice");
        Vertex v2 = new Vertex("person");
        v2.id("v2");
        v2.property("name", "Bob");
        // Server-side paging artifact: the boundary record repeats back-to-back —
        // v1 tails page 1 and heads page 2.
        client.vertexPages.add(new PageResult<>(java.util.Arrays.asList(v2, v1), "next"));
        client.vertexPages.add(new PageResult<>(java.util.Arrays.asList(v1, v2), null));
        CountingContext context = new CountingContext();
        ListCollector collector = new ListCollector();

        HugeGraphSourceReader reader =
                new HugeGraphSourceReader(
                        new SingleSplitReaderContext(context),
                        sourceConfig(MappingConfig.LabelType.VERTEX, propertyRowType),
                        HugeGraphSourceFactory.prependReservedFields(
                                propertyRowType, MappingConfig.LabelType.VERTEX),
                        client);

        reader.pollNext(collector);
        reader.pollNext(collector);

        // 4 raw records, 1 adjacent duplicate skipped; non-adjacent repeat of v2 is kept
        assertEquals(3, collector.rows.size());
        assertEquals("v2", collector.rows.get(0).getField(0));
        assertEquals("v1", collector.rows.get(1).getField(0));
        assertEquals("v2", collector.rows.get(2).getField(0));
    }

    @Test
    void testEmptyIntermediatePageContinues() {
        SeaTunnelRowType propertyRowType = propertyRowType();
        FakeHugeGraphOperations client = new FakeHugeGraphOperations();
        Vertex vertex = new Vertex("person");
        vertex.id("v1");
        vertex.property("name", "Alice");
        client.vertexPages.add(new PageResult<>(Collections.emptyList(), "next"));
        client.vertexPages.add(new PageResult<>(Collections.singletonList(vertex), null));
        CountingContext context = new CountingContext();
        ListCollector collector = new ListCollector();
        HugeGraphSourceReader reader =
                new HugeGraphSourceReader(
                        new SingleSplitReaderContext(context),
                        sourceConfig(MappingConfig.LabelType.VERTEX, propertyRowType),
                        HugeGraphSourceFactory.prependReservedFields(
                                propertyRowType, MappingConfig.LabelType.VERTEX),
                        client);

        reader.pollNext(collector);
        reader.pollNext(collector);

        assertEquals(1, collector.rows.size());
        assertEquals(1, context.noMoreElementCount);
    }

    @Test
    void testRepeatedPageMarkerFails() {
        SeaTunnelRowType propertyRowType = propertyRowType();
        FakeHugeGraphOperations client = new FakeHugeGraphOperations();
        client.vertexPages.add(new PageResult<>(Collections.emptyList(), "next"));
        client.vertexPages.add(new PageResult<>(Collections.emptyList(), "next"));
        HugeGraphSourceReader reader =
                new HugeGraphSourceReader(
                        new SingleSplitReaderContext(new CountingContext()),
                        sourceConfig(MappingConfig.LabelType.VERTEX, propertyRowType),
                        HugeGraphSourceFactory.prependReservedFields(
                                propertyRowType, MappingConfig.LabelType.VERTEX),
                        client);
        ListCollector collector = new ListCollector();

        reader.pollNext(collector);

        assertThrows(HugeGraphConnectorException.class, () -> reader.pollNext(collector));
    }

    @Test
    void testPaginationStateRestoresAtNextPage() throws Exception {
        SeaTunnelRowType propertyRowType = propertyRowType();
        FakeHugeGraphOperations firstClient = new FakeHugeGraphOperations();
        Vertex first = new Vertex("person");
        first.id("v1");
        first.property("name", "Alice");
        firstClient.vertexPages.add(new PageResult<>(Collections.singletonList(first), "next"));
        HugeGraphSourceReader firstReader =
                new HugeGraphSourceReader(
                        new SingleSplitReaderContext(new CountingContext()),
                        sourceConfig(MappingConfig.LabelType.VERTEX, propertyRowType),
                        HugeGraphSourceFactory.prependReservedFields(
                                propertyRowType, MappingConfig.LabelType.VERTEX),
                        firstClient);
        firstReader.pollNext(new ListCollector());

        FakeHugeGraphOperations restoredClient = new FakeHugeGraphOperations();
        Vertex second = new Vertex("person");
        second.id("v2");
        second.property("name", "Bob");
        restoredClient.vertexPages.add(new PageResult<>(Collections.singletonList(second), null));
        CountingContext restoredContext = new CountingContext();
        HugeGraphSourceReader restoredReader =
                new HugeGraphSourceReader(
                        new SingleSplitReaderContext(restoredContext),
                        sourceConfig(MappingConfig.LabelType.VERTEX, propertyRowType),
                        HugeGraphSourceFactory.prependReservedFields(
                                propertyRowType, MappingConfig.LabelType.VERTEX),
                        restoredClient);
        restoredReader.addSplits(firstReader.snapshotState(1L));
        ListCollector restoredCollector = new ListCollector();

        restoredReader.pollNext(restoredCollector);

        assertEquals("next", restoredClient.requestedPages.get(0));
        assertEquals("v2", restoredCollector.rows.get(0).getField(0));
        assertEquals(1, restoredContext.noMoreElementCount);
    }

    @Test
    void testFinishedStateSignalsNoMoreElementAfterRestore() throws Exception {
        SeaTunnelRowType propertyRowType = propertyRowType();
        FakeHugeGraphOperations firstClient = new FakeHugeGraphOperations();
        firstClient.vertexPages.add(new PageResult<>(Collections.emptyList(), null));
        HugeGraphSourceReader firstReader =
                new HugeGraphSourceReader(
                        new SingleSplitReaderContext(new CountingContext()),
                        sourceConfig(MappingConfig.LabelType.VERTEX, propertyRowType),
                        HugeGraphSourceFactory.prependReservedFields(
                                propertyRowType, MappingConfig.LabelType.VERTEX),
                        firstClient);
        firstReader.pollNext(new ListCollector());

        FakeHugeGraphOperations restoredClient = new FakeHugeGraphOperations();
        CountingContext restoredContext = new CountingContext();
        HugeGraphSourceReader restoredReader =
                new HugeGraphSourceReader(
                        new SingleSplitReaderContext(restoredContext),
                        sourceConfig(MappingConfig.LabelType.VERTEX, propertyRowType),
                        HugeGraphSourceFactory.prependReservedFields(
                                propertyRowType, MappingConfig.LabelType.VERTEX),
                        restoredClient);
        restoredReader.addSplits(firstReader.snapshotState(1L));

        restoredReader.pollNext(new ListCollector());

        assertEquals(1, restoredContext.noMoreElementCount);
        assertTrue(restoredClient.requestedPages.isEmpty());
    }

    @Test
    void testEdgeReservedFieldsAreStrings() {
        SeaTunnelRowType propertyRowType = propertyRowType();
        FakeHugeGraphOperations client = new FakeHugeGraphOperations();
        Edge edge = new Edge("knows");
        edge.id("S1>knows>S2");
        edge.sourceId(1L);
        edge.sourceLabel("person");
        edge.targetId(2L);
        edge.targetLabel("person");
        edge.property("name", "since");
        client.edgePages.add(new PageResult<>(Collections.singletonList(edge), null));
        CountingContext context = new CountingContext();
        ListCollector collector = new ListCollector();

        HugeGraphSourceReader reader =
                new HugeGraphSourceReader(
                        new SingleSplitReaderContext(context),
                        sourceConfig(MappingConfig.LabelType.EDGE, propertyRowType),
                        HugeGraphSourceFactory.prependReservedFields(
                                propertyRowType, MappingConfig.LabelType.EDGE),
                        client);

        reader.internalPollNext(collector);

        assertEquals(1, context.noMoreElementCount);
        assertEquals(1, collector.rows.size());
        assertArrayEquals(
                new Object[] {"S1>knows>S2", "knows", "1", "person", "2", "person", "since", null},
                collector.rows.get(0).getFields());
    }

    @Test
    void testRestDecodedPropertyValuesAreNormalizedToSeaTunnelTypes() {
        SeaTunnelRowType propertyRowType =
                new SeaTunnelRowType(
                        new String[] {"count", "ratio", "created_at", "payload"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.LONG_TYPE,
                            BasicType.FLOAT_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE,
                            PrimitiveByteArrayType.INSTANCE
                        });
        FakeHugeGraphOperations client = new FakeHugeGraphOperations();
        Vertex vertex = new Vertex("metric");
        vertex.id("v1");
        vertex.property("count", Integer.valueOf(1));
        vertex.property("ratio", Double.valueOf(0.5D));
        vertex.property("created_at", Long.valueOf(1000L));
        vertex.property("payload", Base64.getEncoder().encodeToString(new byte[] {1, 2, 3}));
        client.vertexPages.add(new PageResult<>(Collections.singletonList(vertex), null));
        ListCollector collector = new ListCollector();

        HugeGraphSourceReader reader =
                new HugeGraphSourceReader(
                        new SingleSplitReaderContext(new CountingContext()),
                        sourceConfig(MappingConfig.LabelType.VERTEX, propertyRowType),
                        HugeGraphSourceFactory.prependReservedFields(
                                propertyRowType, MappingConfig.LabelType.VERTEX),
                        client);

        reader.internalPollNext(collector);

        Object[] fields = collector.rows.get(0).getFields();
        assertInstanceOf(Long.class, fields[2]);
        assertEquals(1L, fields[2]);
        assertInstanceOf(Float.class, fields[3]);
        assertEquals(0.5F, fields[3]);
        assertEquals(
                java.time.LocalDateTime.ofInstant(
                        java.time.Instant.ofEpochMilli(1000L), java.time.ZoneId.systemDefault()),
                fields[4]);
        assertArrayEquals(new byte[] {1, 2, 3}, (byte[]) fields[5]);
    }

    @Test
    void testDatePropertyReturnedAsSpaceSeparatedStringIsParsed() {
        // HugeGraph server serializes DATE as "yyyy-MM-dd HH:mm:ss.SSS" (space separator),
        // which LocalDateTime.parse (ISO 'T' only) rejects. The reader must accept it.
        SeaTunnelRowType propertyRowType =
                new SeaTunnelRowType(
                        new String[] {"created"},
                        new SeaTunnelDataType<?>[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});
        FakeHugeGraphOperations client = new FakeHugeGraphOperations();
        Vertex vertex = new Vertex("acct");
        vertex.id("v1");
        vertex.property("created", "2026-09-11 23:20:11.000");
        client.vertexPages.add(new PageResult<>(Collections.singletonList(vertex), null));
        ListCollector collector = new ListCollector();

        HugeGraphSourceReader reader =
                new HugeGraphSourceReader(
                        new SingleSplitReaderContext(new CountingContext()),
                        sourceConfig(MappingConfig.LabelType.VERTEX, propertyRowType),
                        HugeGraphSourceFactory.prependReservedFields(
                                propertyRowType, MappingConfig.LabelType.VERTEX),
                        client);

        reader.internalPollNext(collector);

        assertEquals(
                java.time.LocalDateTime.of(2026, 9, 11, 23, 20, 11),
                collector.rows.get(0).getField(2));
    }

    private HugeGraphSourceConfig sourceConfig(
            MappingConfig.LabelType labelType, SeaTunnelRowType propertyRowType) {
        HugeGraphSourceConfig config = new HugeGraphSourceConfig();
        config.setLabel(labelType == MappingConfig.LabelType.VERTEX ? "person" : "knows");
        config.setLabelType(labelType);
        config.setSchema(propertyRowType);
        config.setPageSize(100);
        return config;
    }

    private SeaTunnelRowType propertyRowType() {
        return new SeaTunnelRowType(
                new String[] {"name", "age"},
                new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});
    }

    private static class FakeHugeGraphOperations implements HugeGraphOperations {
        private final List<PageResult<Vertex>> vertexPages = new ArrayList<>();
        private final List<PageResult<Edge>> edgePages = new ArrayList<>();
        private final List<String> requestedPages = new ArrayList<>();
        private final Map<String, DataType> propertyTypes = new HashMap<>();
        private final Map<String, Cardinality> propertyCardinalities = new HashMap<>();
        private Set<String> vertexProperties;
        private Set<String> edgeProperties;

        @Override
        public Set<String> getVertexLabelPropertiesOrNull(String label) {
            return vertexProperties;
        }

        @Override
        public Set<String> getEdgeLabelPropertiesOrNull(String label) {
            return edgeProperties;
        }

        @Override
        public DataType getPropertyDataType(String propertyName) {
            return propertyTypes.get(propertyName);
        }

        @Override
        public Cardinality getPropertyCardinality(String propertyName) {
            return propertyCardinalities.getOrDefault(propertyName, Cardinality.SINGLE);
        }

        @Override
        public PageResult<Vertex> listVertices(String label, String page, int limit) {
            requestedPages.add(page);
            return vertexPages.remove(0);
        }

        @Override
        public PageResult<Edge> listEdges(String label, String page, int limit) {
            requestedPages.add(page);
            return edgePages.remove(0);
        }

        @Override
        public void close() {}
    }

    private static class ListCollector implements Collector<SeaTunnelRow> {
        private final List<SeaTunnelRow> rows = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow record) {
            rows.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return this;
        }
    }

    private static class CountingContext implements SourceReader.Context {
        private int noMoreElementCount;

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public void signalNoMoreElement() {
            noMoreElementCount++;
        }

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {}

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }
    }
}

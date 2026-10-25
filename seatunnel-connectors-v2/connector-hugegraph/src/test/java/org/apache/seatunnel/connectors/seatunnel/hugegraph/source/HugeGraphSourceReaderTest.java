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
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphOperations;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.PageResult;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.apache.hugegraph.structure.constant.Cardinality;
import org.apache.hugegraph.structure.constant.DataType;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Shard;
import org.apache.hugegraph.structure.graph.Vertex;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
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
                newReader(MappingConfig.LabelType.VERTEX, propertyRowType, client);

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
                newReader(MappingConfig.LabelType.VERTEX, propertyRowType, client);

        assertThrows(HugeGraphConnectorException.class, reader::open);
    }

    @Test
    void testOpenFailsWhenServerListPropertyDeclaredAsScalar() {
        // Server has LIST but user declared scalar — error must guide the user to array<...>
        // rather than throwing a mid-scan CCE against the scalar row builder.
        FakeHugeGraphOperations client = new FakeHugeGraphOperations();
        client.vertexProperties = new HashSet<>();
        client.vertexProperties.add("name");
        client.vertexProperties.add("age");
        client.propertyTypes.put("name", DataType.TEXT);
        client.propertyTypes.put("age", DataType.INT);
        client.propertyCardinalities.put("age", Cardinality.LIST);
        SeaTunnelRowType propertyRowType = propertyRowType();
        HugeGraphSourceReader reader =
                newReader(MappingConfig.LabelType.VERTEX, propertyRowType, client);

        HugeGraphConnectorException ex =
                assertThrows(HugeGraphConnectorException.class, reader::open);
        assertTrue(ex.getMessage().contains("array<"));
    }

    @Test
    void testOpenFailsWhenServerScalarButUserDeclaredArray() {
        FakeHugeGraphOperations client = new FakeHugeGraphOperations();
        client.vertexProperties = new HashSet<>();
        client.vertexProperties.add("tags");
        client.propertyTypes.put("tags", DataType.TEXT);
        // cardinality not set → SINGLE (default)
        SeaTunnelRowType propertyRowType =
                new SeaTunnelRowType(
                        new String[] {"tags"},
                        new SeaTunnelDataType<?>[] {ArrayType.STRING_ARRAY_TYPE});
        HugeGraphSourceReader reader =
                newReader(MappingConfig.LabelType.VERTEX, propertyRowType, client);

        assertThrows(HugeGraphConnectorException.class, reader::open);
    }

    @Test
    void testListPropertyIsReadAsArray() throws Exception {
        SeaTunnelRowType propertyRowType =
                new SeaTunnelRowType(
                        new String[] {"tags"},
                        new SeaTunnelDataType<?>[] {ArrayType.STRING_ARRAY_TYPE});
        FakeHugeGraphOperations client = new FakeHugeGraphOperations();
        client.vertexProperties = new HashSet<>();
        client.vertexProperties.add("tags");
        client.propertyTypes.put("tags", DataType.TEXT);
        client.propertyCardinalities.put("tags", Cardinality.LIST);

        Vertex vertex = new Vertex("person");
        vertex.id("v1");
        vertex.property("tags", Arrays.asList("a", "b", "c"));
        client.vertexPages.add(new PageResult<>(Collections.singletonList(vertex), null));
        ListCollector collector = new ListCollector();
        HugeGraphSourceReader reader =
                newReader(MappingConfig.LabelType.VERTEX, propertyRowType, client);

        reader.open();
        reader.addSplits(listSplit());
        reader.pollNext(collector);

        assertEquals(1, collector.rows.size());
        Object cell = collector.rows.get(0).getField(2);
        assertInstanceOf(String[].class, cell);
        assertArrayEquals(new String[] {"a", "b", "c"}, (String[]) cell);
    }

    @Test
    void testSetPropertyIsReadAsArray() throws Exception {
        // SET cardinality is accepted; element order is not guaranteed by the server, but the
        // reader must not fail and must produce a typed array of the server's element type.
        SeaTunnelRowType propertyRowType =
                new SeaTunnelRowType(
                        new String[] {"tags"},
                        new SeaTunnelDataType<?>[] {ArrayType.INT_ARRAY_TYPE});
        FakeHugeGraphOperations client = new FakeHugeGraphOperations();
        client.vertexProperties = new HashSet<>();
        client.vertexProperties.add("tags");
        client.propertyTypes.put("tags", DataType.INT);
        client.propertyCardinalities.put("tags", Cardinality.SET);

        Vertex vertex = new Vertex("person");
        vertex.id("v1");
        java.util.LinkedHashSet<Integer> serverValue = new java.util.LinkedHashSet<>();
        serverValue.add(10);
        serverValue.add(20);
        vertex.property("tags", serverValue);
        client.vertexPages.add(new PageResult<>(Collections.singletonList(vertex), null));
        ListCollector collector = new ListCollector();
        HugeGraphSourceReader reader =
                newReader(MappingConfig.LabelType.VERTEX, propertyRowType, client);

        reader.open();
        reader.addSplits(listSplit());
        reader.pollNext(collector);

        Object cell = collector.rows.get(0).getField(2);
        assertInstanceOf(Integer[].class, cell);
        assertEquals(2, ((Integer[]) cell).length);
    }

    @Test
    void testVertexPagingAndNullProperties() throws Exception {
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
                newReader(context, MappingConfig.LabelType.VERTEX, propertyRowType, client);
        reader.addSplits(listSplit());
        reader.handleNoMoreSplits();

        drain(reader, collector, context);

        assertEquals(1, context.noMoreElementCount);
        assertEquals(1, collector.rows.size());
        assertArrayEquals(
                new Object[] {"v1", "person", "Alice", null}, collector.rows.get(0).getFields());
        // page1 requested with the null first page, page2 with the "next" marker
        assertEquals("next", client.requestedPages.get(1));
    }

    @Test
    void testEmptyLabelProducesNoRowsAndFinishes() throws Exception {
        SeaTunnelRowType propertyRowType = propertyRowType();
        FakeHugeGraphOperations client = new FakeHugeGraphOperations();
        client.vertexPages.add(new PageResult<>(Collections.emptyList(), null));
        CountingContext context = new CountingContext();
        ListCollector collector = new ListCollector();
        HugeGraphSourceReader reader =
                newReader(context, MappingConfig.LabelType.VERTEX, propertyRowType, client);
        reader.addSplits(listSplit());
        reader.handleNoMoreSplits();

        drain(reader, collector, context);

        assertTrue(collector.rows.isEmpty());
        assertEquals(1, context.noMoreElementCount);
    }

    @Test
    void testAdjacentServerPagingDuplicatesAreSkipped() throws Exception {
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
                newReader(context, MappingConfig.LabelType.VERTEX, propertyRowType, client);
        reader.addSplits(listSplit());
        reader.handleNoMoreSplits();

        drain(reader, collector, context);

        // 4 raw records, 1 adjacent duplicate skipped; non-adjacent repeat of v2 is kept
        assertEquals(3, collector.rows.size());
        assertEquals("v2", collector.rows.get(0).getField(0));
        assertEquals("v1", collector.rows.get(1).getField(0));
        assertEquals("v2", collector.rows.get(2).getField(0));
    }

    @Test
    void testEmptyIntermediatePageContinues() throws Exception {
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
                newReader(context, MappingConfig.LabelType.VERTEX, propertyRowType, client);
        reader.addSplits(listSplit());
        reader.handleNoMoreSplits();

        drain(reader, collector, context);

        assertEquals(1, collector.rows.size());
        assertEquals(1, context.noMoreElementCount);
    }

    @Test
    void testRepeatedPageMarkerFails() throws Exception {
        SeaTunnelRowType propertyRowType = propertyRowType();
        FakeHugeGraphOperations client = new FakeHugeGraphOperations();
        client.vertexPages.add(new PageResult<>(Collections.emptyList(), "next"));
        client.vertexPages.add(new PageResult<>(Collections.emptyList(), "next"));
        HugeGraphSourceReader reader =
                newReader(MappingConfig.LabelType.VERTEX, propertyRowType, client);
        reader.addSplits(listSplit());
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
                newReader(MappingConfig.LabelType.VERTEX, propertyRowType, firstClient);
        firstReader.addSplits(listSplit());
        firstReader.pollNext(new ListCollector());

        FakeHugeGraphOperations restoredClient = new FakeHugeGraphOperations();
        Vertex second = new Vertex("person");
        second.id("v2");
        second.property("name", "Bob");
        restoredClient.vertexPages.add(new PageResult<>(Collections.singletonList(second), null));
        CountingContext restoredContext = new CountingContext();
        HugeGraphSourceReader restoredReader =
                newReader(
                        restoredContext,
                        MappingConfig.LabelType.VERTEX,
                        propertyRowType,
                        restoredClient);
        restoredReader.addSplits(firstReader.snapshotState(1L));
        restoredReader.handleNoMoreSplits();
        ListCollector restoredCollector = new ListCollector();

        drain(restoredReader, restoredCollector, restoredContext);

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
                newReader(MappingConfig.LabelType.VERTEX, propertyRowType, firstClient);
        firstReader.addSplits(listSplit());
        firstReader.pollNext(new ListCollector());

        FakeHugeGraphOperations restoredClient = new FakeHugeGraphOperations();
        CountingContext restoredContext = new CountingContext();
        HugeGraphSourceReader restoredReader =
                newReader(
                        restoredContext,
                        MappingConfig.LabelType.VERTEX,
                        propertyRowType,
                        restoredClient);
        restoredReader.addSplits(firstReader.snapshotState(1L));
        restoredReader.handleNoMoreSplits();

        drain(restoredReader, new ListCollector(), restoredContext);

        assertEquals(1, restoredContext.noMoreElementCount);
        assertTrue(restoredClient.requestedPages.isEmpty());
    }

    @Test
    void testEdgeReservedFieldsAreStrings() throws Exception {
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
                newReader(context, MappingConfig.LabelType.EDGE, propertyRowType, client);
        reader.addSplits(listSplit());
        reader.pollNext(collector);

        assertEquals(1, collector.rows.size());
        assertArrayEquals(
                new Object[] {"S1>knows>S2", "knows", "1", "person", "2", "person", "since", null},
                collector.rows.get(0).getFields());
    }

    @Test
    void testRestDecodedPropertyValuesAreNormalizedToSeaTunnelTypes() throws Exception {
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
                newReader(MappingConfig.LabelType.VERTEX, propertyRowType, client);
        reader.addSplits(listSplit());
        reader.pollNext(collector);

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
    void testDatePropertyReturnedAsSpaceSeparatedStringIsParsed() throws Exception {
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
                newReader(MappingConfig.LabelType.VERTEX, propertyRowType, client);
        reader.addSplits(listSplit());
        reader.pollNext(collector);

        assertEquals(
                java.time.LocalDateTime.of(2026, 9, 11, 23, 20, 11),
                collector.rows.get(0).getField(2));
    }

    @Test
    void testFilterIsForwardedToClient() throws Exception {
        FakeHugeGraphOperations client = new FakeHugeGraphOperations();
        client.vertexProperties = new HashSet<>(Arrays.asList("name", "age"));
        client.propertyTypes.put("name", DataType.TEXT);
        client.propertyTypes.put("age", DataType.INT);
        Vertex vertex = new Vertex("person");
        vertex.id("v1");
        vertex.property("name", "alice");
        vertex.property("age", 30);
        client.vertexPages.add(new PageResult<>(Collections.singletonList(vertex), null));

        SeaTunnelRowType propertyRowType = propertyRowType();
        HugeGraphSourceConfig config =
                sourceConfig(MappingConfig.LabelType.VERTEX, propertyRowType);
        Map<String, Object> filter = new HashMap<>();
        filter.put("name", "alice");
        config.setFilter(filter);

        HugeGraphSourceReader reader =
                new HugeGraphSourceReader(
                        new CountingContext(),
                        config,
                        singleContext("person", propertyRowType, MappingConfig.LabelType.VERTEX),
                        client);
        reader.open();
        reader.addSplits(listSplit());
        reader.pollNext(new ListCollector());

        assertEquals(filter, client.capturedFilter);
    }

    @Test
    void testOpenFailsWhenFilterPropertyNotOnLabel() {
        FakeHugeGraphOperations client = new FakeHugeGraphOperations();
        client.vertexProperties = new HashSet<>(Arrays.asList("name", "age"));
        client.propertyTypes.put("name", DataType.TEXT);
        client.propertyTypes.put("age", DataType.INT);

        SeaTunnelRowType propertyRowType = propertyRowType();
        HugeGraphSourceConfig config =
                sourceConfig(MappingConfig.LabelType.VERTEX, propertyRowType);
        Map<String, Object> filter = new HashMap<>();
        filter.put("nonexistent", "x");
        config.setFilter(filter);

        HugeGraphSourceReader reader =
                new HugeGraphSourceReader(
                        new CountingContext(),
                        config,
                        singleContext("person", propertyRowType, MappingConfig.LabelType.VERTEX),
                        client);

        assertThrows(HugeGraphConnectorException.class, reader::open);
    }

    @Test
    void testShardModeScansShardAndFiltersByLabel() throws Exception {
        // A shard scan returns vertices of ALL labels in the key range; the reader must keep only
        // the configured label ("person") and drop the others ("company").
        SeaTunnelRowType propertyRowType = propertyRowType();
        FakeHugeGraphOperations client = new FakeHugeGraphOperations();
        Vertex person = new Vertex("person");
        person.id("p1");
        person.property("name", "Alice");
        Vertex company = new Vertex("company");
        company.id("c1");
        Vertex person2 = new Vertex("person");
        person2.id("p2");
        person2.property("name", "Bob");
        client.scanVertexPages.add(
                new PageResult<>(java.util.Arrays.asList(person, company, person2), null));
        CountingContext context = new CountingContext();
        ListCollector collector = new ListCollector();

        HugeGraphSourceReader reader =
                newReader(context, MappingConfig.LabelType.VERTEX, propertyRowType, client);
        reader.addSplits(
                Collections.singletonList(
                        HugeGraphSourceSplit.shardSplit("shard-0", new Shard("0", "9", 0L))));
        reader.handleNoMoreSplits();

        drain(reader, collector, context);

        assertEquals(2, collector.rows.size());
        assertEquals("p1", collector.rows.get(0).getField(0));
        assertEquals("p2", collector.rows.get(1).getField(0));
        assertEquals(1, context.noMoreElementCount);
    }

    @Test
    void testMultipleShardSplitsAreAllDrained() throws Exception {
        SeaTunnelRowType propertyRowType = propertyRowType();
        FakeHugeGraphOperations client = new FakeHugeGraphOperations();
        Vertex a = new Vertex("person");
        a.id("a");
        Vertex b = new Vertex("person");
        b.id("b");
        client.scanVertexPages.add(new PageResult<>(Collections.singletonList(a), null));
        client.scanVertexPages.add(new PageResult<>(Collections.singletonList(b), null));
        CountingContext context = new CountingContext();
        ListCollector collector = new ListCollector();

        HugeGraphSourceReader reader =
                newReader(context, MappingConfig.LabelType.VERTEX, propertyRowType, client);
        reader.addSplits(
                Arrays.asList(
                        HugeGraphSourceSplit.shardSplit("shard-0", new Shard("0", "5", 0L)),
                        HugeGraphSourceSplit.shardSplit("shard-1", new Shard("5", "9", 0L))));
        reader.handleNoMoreSplits();

        drain(reader, collector, context);

        assertEquals(2, collector.rows.size());
        assertEquals(1, context.noMoreElementCount);
    }

    @Test
    void filterValueCoercedToPropertyType() {
        // A BOOLEAN property filtered with the string "true" must become a real Boolean, and a LONG
        // filtered with a loosely-typed value must become a Long — otherwise the server matches by
        // typed value and silently returns 0 rows.
        assertEquals(
                Boolean.TRUE,
                HugeGraphSourceReader.coerceFilterValue("active", "true", DataType.BOOLEAN));
        assertEquals(
                Boolean.FALSE,
                HugeGraphSourceReader.coerceFilterValue("active", "FALSE", DataType.BOOLEAN));
        assertEquals(7L, HugeGraphSourceReader.coerceFilterValue("count", "7", DataType.LONG));
        assertEquals(7L, HugeGraphSourceReader.coerceFilterValue("count", 7, DataType.LONG));
        assertEquals("x", HugeGraphSourceReader.coerceFilterValue("name", "x", DataType.TEXT));
    }

    @Test
    void filterValueThatCannotCoerceFailsFast() {
        HugeGraphConnectorException ex =
                assertThrows(
                        HugeGraphConnectorException.class,
                        () ->
                                HugeGraphSourceReader.coerceFilterValue(
                                        "active", "yes", DataType.BOOLEAN));
        assertTrue(ex.getMessage().contains("active"));
    }

    @Test
    void readAllTagsRowsWithPerLabelTableId() throws Exception {
        // Read-all mode: two label-list splits (person, software). Each split reads exactly its
        // label and every emitted row must carry that label's tableId so a downstream multi-table
        // sink can route it; the produced row uses that label's own row type.
        FakeHugeGraphOperations client = new FakeHugeGraphOperations();
        Vertex person = new Vertex("person");
        person.id("p1");
        person.property("name", "Alice");
        Vertex software = new Vertex("software");
        software.id("s1");
        software.property("name", "SeaTunnel");
        client.vertexPages.add(new PageResult<>(Collections.singletonList(person), null));
        client.vertexPages.add(new PageResult<>(Collections.singletonList(software), null));

        SeaTunnelRowType props =
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        Map<String, LabelTableContext> contexts = new HashMap<>();
        contexts.putAll(singleContext("person", props, MappingConfig.LabelType.VERTEX));
        contexts.putAll(singleContext("software", props, MappingConfig.LabelType.VERTEX));

        HugeGraphSourceConfig config = new HugeGraphSourceConfig();
        config.setReadAllLabels(true);
        config.setLabel(null);
        config.setLabels(Arrays.asList("person", "software"));
        config.setLabelType(MappingConfig.LabelType.VERTEX);
        config.setPageSize(100);

        CountingContext context = new CountingContext();
        ListCollector collector = new ListCollector();
        HugeGraphSourceReader reader = new HugeGraphSourceReader(context, config, contexts, client);
        reader.open(); // read-all skips single-label schema validation
        reader.addSplits(
                Arrays.asList(
                        HugeGraphSourceSplit.labelListSplit("label-list-person", "person"),
                        HugeGraphSourceSplit.labelListSplit("label-list-software", "software")));
        reader.handleNoMoreSplits();

        drain(reader, collector, context);

        assertEquals(2, collector.rows.size());
        assertEquals("person", collector.rows.get(0).getTableId());
        assertEquals("Alice", collector.rows.get(0).getField(2));
        assertEquals("software", collector.rows.get(1).getTableId());
        assertEquals("SeaTunnel", collector.rows.get(1).getField(2));
    }

    private static void drain(
            HugeGraphSourceReader reader, ListCollector collector, CountingContext context)
            throws Exception {
        int guard = 0;
        while (context.noMoreElementCount == 0 && guard++ < 100) {
            reader.pollNext(collector);
        }
    }

    private static List<HugeGraphSourceSplit> listSplit() {
        // null label => reader falls back to the single configured label (as a shard split does).
        return Collections.singletonList(HugeGraphSourceSplit.labelListSplit("label-list", null));
    }

    private HugeGraphSourceReader newReader(
            MappingConfig.LabelType labelType,
            SeaTunnelRowType propertyRowType,
            HugeGraphOperations client) {
        return newReader(new CountingContext(), labelType, propertyRowType, client);
    }

    private HugeGraphSourceReader newReader(
            CountingContext context,
            MappingConfig.LabelType labelType,
            SeaTunnelRowType propertyRowType,
            HugeGraphOperations client) {
        HugeGraphSourceConfig config = sourceConfig(labelType, propertyRowType);
        return new HugeGraphSourceReader(
                context,
                config,
                singleContext(config.getLabel(), propertyRowType, labelType),
                client);
    }

    private static Map<String, LabelTableContext> singleContext(
            String label, SeaTunnelRowType propertyRowType, MappingConfig.LabelType labelType) {
        SeaTunnelRowType outputRowType =
                HugeGraphSourceFactory.prependReservedFields(propertyRowType, labelType);
        Map<String, LabelTableContext> contexts = new HashMap<>();
        contexts.put(label, new LabelTableContext(label, propertyRowType, outputRowType, label));
        return contexts;
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
        private final List<PageResult<Vertex>> scanVertexPages = new ArrayList<>();
        private final List<PageResult<Edge>> scanEdgePages = new ArrayList<>();
        private final List<String> requestedPages = new ArrayList<>();
        private final Map<String, DataType> propertyTypes = new HashMap<>();
        private final Map<String, Cardinality> propertyCardinalities = new HashMap<>();
        private Set<String> vertexProperties;
        private Set<String> edgeProperties;
        private Map<String, Object> capturedFilter;

        @Override
        public Set<String> getVertexLabelPropertiesOrNull(String label) {
            return vertexProperties;
        }

        @Override
        public Set<String> getEdgeLabelPropertiesOrNull(String label) {
            return edgeProperties;
        }

        @Override
        public List<String> listVertexLabels() {
            return Collections.emptyList();
        }

        @Override
        public List<String> listEdgeLabels() {
            return Collections.emptyList();
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
        public PageResult<Vertex> listVertices(
                String label, java.util.Map<String, Object> filter, String page, int limit) {
            requestedPages.add(page);
            capturedFilter = filter;
            return vertexPages.remove(0);
        }

        @Override
        public PageResult<Edge> listEdges(
                String label, java.util.Map<String, Object> filter, String page, int limit) {
            requestedPages.add(page);
            capturedFilter = filter;
            return edgePages.remove(0);
        }

        @Override
        public List<Shard> vertexShards(long splitSize) {
            return Collections.emptyList();
        }

        @Override
        public List<Shard> edgeShards(long splitSize) {
            return Collections.emptyList();
        }

        @Override
        public PageResult<Vertex> scanVertices(Shard shard, String page, int limit) {
            requestedPages.add(page);
            return scanVertexPages.remove(0);
        }

        @Override
        public PageResult<Edge> scanEdges(Shard shard, String page, int limit) {
            requestedPages.add(page);
            return scanEdgePages.remove(0);
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

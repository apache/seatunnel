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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.mapper;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;

import org.apache.hugegraph.structure.constant.Frequency;
import org.apache.hugegraph.structure.constant.IdStrategy;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Locks the HugeGraph server-side 5-part EdgeId format used on DELETE. Regressions here silently
 * target the wrong edge (or none), so the exact layout is pinned by these assertions. Expected
 * strings were captured from HugeGraph's own {@code SplicingIdGenerator} (client 1.5.0).
 */
class EdgeMapperTest {

    @Test
    void testSingleFrequencyStringEndpoints() {
        // {S}{owner}>{labelId}>{subLabelId}>{sortValues=empty}>{S}{other}
        assertEquals(
                "S1:marko>1>1>>S1:david",
                EdgeMapper.spliceEdgeId("1:marko", "1:david", "1", Collections.emptyList()));
    }

    @Test
    void testMultipleFrequencyPopulatesSortValuesSegment() {
        assertEquals(
                "S1:bob>2>2>2024-01-01>S3:proj",
                EdgeMapper.spliceEdgeId(
                        "1:bob", "3:proj", "2", Collections.singletonList("2024-01-01")));
    }

    @Test
    void testNumberEndpointsUseLPrefix() {
        assertEquals(
                "L123>5>5>>L456",
                EdgeMapper.spliceEdgeId(123L, 456L, "5", Collections.emptyList()));
    }

    @Test
    void testUuidEndpointsUseUPrefix() {
        UUID src = UUID.fromString("12345678-1234-1234-1234-123456789abc");
        UUID tgt = UUID.fromString("87654321-4321-4321-4321-cba987654321");
        assertEquals(
                "U12345678-1234-1234-1234-123456789abc>9>9>>U87654321-4321-4321-4321-cba987654321",
                EdgeMapper.spliceEdgeId(src, tgt, "9", Collections.emptyList()));
    }

    @Test
    void testCompositeSortValuesJoinedByBang() {
        assertEquals(
                "S1:a>7>7>x!y>S1:b",
                EdgeMapper.spliceEdgeId("1:a", "1:b", "7", Arrays.asList("x", "y")));
    }

    @Test
    void testSortValueContainingSeparatorIsEscaped() {
        // A single sort value that literally contains '!' must be backtick-escaped so it is not
        // read back as two values.
        assertEquals(
                "S1:a>7>7>x`!y>S1:b",
                EdgeMapper.spliceEdgeId("1:a", "1:b", "7", Collections.singletonList("x!y")));
    }

    @Test
    void testVertexIdContainingSeparatorIsEscaped() {
        // A vertex id that contains the segment separator '>' must be backtick-escaped so the id
        // still parses into the correct 5 segments.
        assertEquals(
                "S1:a`>b>7>7>>S1:c",
                EdgeMapper.spliceEdgeId("1:a>b", "1:c", "7", Collections.emptyList()));
    }

    @Test
    void testRawIdPassthroughPrimaryKeyEndpointsKeepStringPrefix() {
        // ~source_id/~target_id carry the already-assembled endpoint ids ("2:alice"); a
        // PRIMARY_KEY endpoint re-applies the 'S' prefix, matching the normal splicing path.
        EdgeMapper mapper = rawPassthroughEdgeMapper(IdStrategy.PRIMARY_KEY);
        Object id = mapper.extractId(new SeaTunnelRow(new Object[] {"2:alice", "2:bob"}));
        assertEquals("S2:alice>1>1>>S2:bob", id);
    }

    @Test
    void testRawIdPassthroughNumberEndpointsUseLPrefix() {
        // A CUSTOMIZE_NUMBER endpoint: the ~source_id string is parsed back to a long so the 'L'
        // prefix is restored.
        EdgeMapper mapper = rawPassthroughEdgeMapper(IdStrategy.CUSTOMIZE_NUMBER);
        Object id = mapper.extractId(new SeaTunnelRow(new Object[] {"123", "456"}));
        assertEquals("L123>1>1>>L456", id);
    }

    @Test
    void testRawIdPassthroughSkipsWhenEndpointIdNull() {
        EdgeMapper mapper = rawPassthroughEdgeMapper(IdStrategy.PRIMARY_KEY);
        assertEquals(null, mapper.extractId(new SeaTunnelRow(new Object[] {null, "2:bob"})));
    }

    private static EdgeMapper rawPassthroughEdgeMapper(IdStrategy endpointStrategy) {
        HugeGraphClient client = mock(HugeGraphClient.class);
        when(client.getEdgeLabelId("knows")).thenReturn("1");
        when(client.getVertexLabelId("person")).thenReturn("2");
        when(client.getIdStrategy("person")).thenReturn(endpointStrategy);
        when(client.getPropertyKeyOrNull(anyString())).thenReturn(null);

        MappingConfig mapping = new MappingConfig();
        mapping.setType(MappingConfig.LabelType.EDGE);
        mapping.setLabel("knows");
        mapping.setSourceConfig(endpoint("person", "~source_id"));
        mapping.setTargetConfig(endpoint("person", "~target_id"));
        mapping.setFrequency(Frequency.SINGLE);

        Map<String, Integer> fields = new LinkedHashMap<>();
        fields.put("~source_id", 0);
        fields.put("~target_id", 1);
        return new EdgeMapper(mapping, fields, client);
    }

    private static MappingConfig.SourceTargetConfig endpoint(String label, String idField) {
        MappingConfig.SourceTargetConfig st = new MappingConfig.SourceTargetConfig();
        st.setLabel(label);
        st.setIdFields(Collections.singletonList(idField));
        return st;
    }
}

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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Locks the HugeGraph server-side 5-part EdgeId format used on DELETE. Regressions here silently
 * target the wrong edge (or none), so the exact layout is pinned by these assertions.
 */
class EdgeMapperTest {

    @Test
    void testSingleFrequencyStringEndpoints() {
        // {S}{owner}>{labelId}>{subLabelId}>{sortValues=empty}>{S}{other}
        assertEquals(
                "S1:marko>1>1>>S1:david", EdgeMapper.spliceEdgeId("1:marko", "1:david", "1", ""));
    }

    @Test
    void testMultipleFrequencyPopulatesSortValuesSegment() {
        assertEquals(
                "S1:bob>2>2>2024-01-01>S3:proj",
                EdgeMapper.spliceEdgeId("1:bob", "3:proj", "2", "2024-01-01"));
    }

    @Test
    void testNumberEndpointsUseLPrefix() {
        assertEquals("L123>5>5>>L456", EdgeMapper.spliceEdgeId(123L, 456L, "5", ""));
    }

    @Test
    void testCompositeSortValuesJoinedByBang() {
        assertEquals("S1:a>7>7>x!y>S1:b", EdgeMapper.spliceEdgeId("1:a", "1:b", "7", "x!y"));
    }
}

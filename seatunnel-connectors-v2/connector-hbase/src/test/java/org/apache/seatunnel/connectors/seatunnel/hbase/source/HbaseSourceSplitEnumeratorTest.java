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

package org.apache.seatunnel.connectors.seatunnel.hbase.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.hbase.client.HbaseClient;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

public class HbaseSourceSplitEnumeratorTest {

    @Mock private SourceSplitEnumerator.Context<HbaseSourceSplit> context;

    @Mock private HbaseClient hbaseClient;

    @Mock private RegionLocator regionLocator;

    @Mock private HbaseParameters hbaseParameters;

    private HbaseSourceSplitEnumerator enumerator;

    @BeforeEach
    void setUp() throws IOException {
        MockitoAnnotations.openMocks(this);

        when(hbaseParameters.getTable()).thenReturn("test_table");
        when(hbaseParameters.getZookeeperQuorum()).thenReturn("127.0.0.1:2801");
        when(hbaseParameters.isBinaryRowkey()).thenReturn(false);
        when(hbaseParameters.getStartRowkey()).thenReturn("");
        when(hbaseParameters.getEndRowkey()).thenReturn("");
        enumerator = new HbaseSourceSplitEnumerator(context, hbaseParameters, hbaseClient);
        when(hbaseClient.getRegionLocator("test_table")).thenReturn(regionLocator);
    }

    @Test
    void testGetTableSplitsWithSingleRegion() throws IOException {
        byte[][] startKeys = {HConstants.EMPTY_BYTE_ARRAY};
        byte[][] endKeys = {HConstants.EMPTY_BYTE_ARRAY};

        when(regionLocator.getStartKeys()).thenReturn(startKeys);
        when(regionLocator.getEndKeys()).thenReturn(endKeys);

        Set<HbaseSourceSplit> splits = enumerator.getTableSplits();

        assertNotNull(splits);
        assertEquals(1, splits.size());

        HbaseSourceSplit split = splits.iterator().next();
        assertEquals("hbase_source_split_0", split.splitId());
        assertArrayEquals(HConstants.EMPTY_BYTE_ARRAY, split.getStartRow());
        assertArrayEquals(HConstants.EMPTY_BYTE_ARRAY, split.getEndRow());
    }

    @Test
    void testGetTableSplitsWithUserDefinedRowKeyRange() throws IOException {
        // Simulate a table with 4 regions but user only wants data from "row100" to "row300"
        byte[][] startKeys = {
            HConstants.EMPTY_BYTE_ARRAY,
            Bytes.toBytes("row050"),
            Bytes.toBytes("row200"),
            Bytes.toBytes("row400")
        };
        byte[][] endKeys = {
            Bytes.toBytes("row050"),
            Bytes.toBytes("row200"),
            Bytes.toBytes("row400"),
            HConstants.EMPTY_BYTE_ARRAY
        };

        when(regionLocator.getStartKeys()).thenReturn(startKeys);
        when(regionLocator.getEndKeys()).thenReturn(endKeys);
        when(hbaseParameters.getStartRowkey()).thenReturn("row100");
        when(hbaseParameters.getEndRowkey()).thenReturn("row300");

        Set<HbaseSourceSplit> splits = enumerator.getTableSplits();

        assertNotNull(splits);
        assertEquals(2, splits.size()); // Should only include regions 1 and 2

        // Verify the splits contain the correct row key ranges
        boolean foundRegion1Split = false, foundRegion2Split = false;
        for (HbaseSourceSplit split : splits) {
            if ("hbase_source_split_1".equals(split.splitId())) {
                foundRegion1Split = true;
                // Start should be user's start key (row100), end should be region end (row200)
                assertArrayEquals(Bytes.toBytes("row100"), split.getStartRow());
                assertArrayEquals(Bytes.toBytes("row200"), split.getEndRow());
            } else if ("hbase_source_split_2".equals(split.splitId())) {
                foundRegion2Split = true;
                // Start should be region start (row200), end should be user's end key (row300)
                assertArrayEquals(Bytes.toBytes("row200"), split.getStartRow());
                assertArrayEquals(Bytes.toBytes("row300"), split.getEndRow());
            }
        }

        assertTrue(foundRegion1Split && foundRegion2Split);
    }

    @Test
    void testGetTableSplitsWithBinaryRowKey() throws IOException {
        byte[][] startKeys = {HConstants.EMPTY_BYTE_ARRAY, new byte[] {0x01, 0x02, 0x03}};
        byte[][] endKeys = {new byte[] {0x01, 0x02, 0x03}, HConstants.EMPTY_BYTE_ARRAY};

        when(regionLocator.getStartKeys()).thenReturn(startKeys);
        when(regionLocator.getEndKeys()).thenReturn(endKeys);
        when(hbaseParameters.isBinaryRowkey()).thenReturn(true);
        when(hbaseParameters.getStartRowkey()).thenReturn("\\x01\\x01\\x01");
        when(hbaseParameters.getEndRowkey()).thenReturn("\\x02\\x02\\x02");

        Set<HbaseSourceSplit> splits = enumerator.getTableSplits();

        assertNotNull(splits);
        assertEquals(2, splits.size());
    }

    @Test
    void testNoMatchingRegionsOfUserEndRowkeyLtRegionStartKey() throws IOException {
        byte[][] startKeys = {
            HConstants.EMPTY_BYTE_ARRAY, Bytes.toBytes("row200"), Bytes.toBytes("row400")
        };
        byte[][] endKeys = {
            Bytes.toBytes("row200"), Bytes.toBytes("row400"), HConstants.EMPTY_BYTE_ARRAY
        };

        when(regionLocator.getStartKeys()).thenReturn(startKeys);
        when(regionLocator.getEndKeys()).thenReturn(endKeys);
        when(hbaseParameters.getStartRowkey()).thenReturn("row10");
        when(hbaseParameters.getEndRowkey()).thenReturn("row15");

        Set<HbaseSourceSplit> splits = enumerator.getTableSplits();

        assertNotNull(splits);
        assertEquals(1, splits.size()); // Should include the first region

        HbaseSourceSplit split = splits.iterator().next();
        assertEquals("hbase_source_split_0", split.splitId());
        assertArrayEquals(Bytes.toBytes("row10"), split.getStartRow());
        assertArrayEquals(Bytes.toBytes("row15"), split.getEndRow());
    }

    @Test
    void testNoMatchingRegionsOfUserStartRowkeyGtRegionEndKey() throws IOException {
        byte[][] startKeys = {
            HConstants.EMPTY_BYTE_ARRAY, Bytes.toBytes("row200"), Bytes.toBytes("row400")
        };
        byte[][] endKeys = {
            Bytes.toBytes("row200"), Bytes.toBytes("row400"), HConstants.EMPTY_BYTE_ARRAY
        };

        when(regionLocator.getStartKeys()).thenReturn(startKeys);
        when(regionLocator.getEndKeys()).thenReturn(endKeys);
        when(hbaseParameters.getStartRowkey()).thenReturn("row500");
        when(hbaseParameters.getEndRowkey()).thenReturn("row600");

        Set<HbaseSourceSplit> splits = enumerator.getTableSplits();

        assertNotNull(splits);
        assertEquals(1, splits.size()); // Should include the last region

        HbaseSourceSplit split = splits.iterator().next();
        assertEquals("hbase_source_split_2", split.splitId());
        assertArrayEquals(Bytes.toBytes("row500"), split.getStartRow());
        assertArrayEquals(Bytes.toBytes("row600"), split.getEndRow());
    }

    @Test
    void testGetTableSplitsWithOnlyStartRowKey() throws IOException {
        byte[][] startKeys = {
            HConstants.EMPTY_BYTE_ARRAY, Bytes.toBytes("row100"), Bytes.toBytes("row200")
        };
        byte[][] endKeys = {
            Bytes.toBytes("row100"), Bytes.toBytes("row200"), HConstants.EMPTY_BYTE_ARRAY
        };

        when(regionLocator.getStartKeys()).thenReturn(startKeys);
        when(regionLocator.getEndKeys()).thenReturn(endKeys);
        when(hbaseParameters.getStartRowkey()).thenReturn("row150");

        Set<HbaseSourceSplit> splits = enumerator.getTableSplits();

        assertNotNull(splits);
        assertEquals(2, splits.size()); // Should include regions 1 and 2

        boolean foundRegion1Split = false, foundRegion2Split = false;
        for (HbaseSourceSplit split : splits) {
            if ("hbase_source_split_1".equals(split.splitId())) {
                foundRegion1Split = true;
                assertArrayEquals(Bytes.toBytes("row150"), split.getStartRow());
                assertArrayEquals(Bytes.toBytes("row200"), split.getEndRow());
            } else if ("hbase_source_split_2".equals(split.splitId())) {
                foundRegion2Split = true;
                assertArrayEquals(Bytes.toBytes("row200"), split.getStartRow());
                assertArrayEquals(HConstants.EMPTY_BYTE_ARRAY, split.getEndRow());
            }
        }

        assertTrue(foundRegion1Split && foundRegion2Split);
    }

    @Test
    void testGetTableSplitsWithOnlyEndRowKey() throws IOException {
        // Test with only end row key specified
        byte[][] startKeys = {
            HConstants.EMPTY_BYTE_ARRAY, Bytes.toBytes("row100"), Bytes.toBytes("row200")
        };
        byte[][] endKeys = {
            Bytes.toBytes("row100"), Bytes.toBytes("row200"), HConstants.EMPTY_BYTE_ARRAY
        };

        when(regionLocator.getStartKeys()).thenReturn(startKeys);
        when(regionLocator.getEndKeys()).thenReturn(endKeys);
        when(hbaseParameters.getEndRowkey()).thenReturn("row150");

        Set<HbaseSourceSplit> splits = enumerator.getTableSplits();

        assertNotNull(splits);
        assertEquals(2, splits.size()); // Should include regions 0 and 1

        boolean foundRegion0Split = false, foundRegion1Split = false;
        for (HbaseSourceSplit split : splits) {
            if ("hbase_source_split_0".equals(split.splitId())) {
                foundRegion0Split = true;
                assertArrayEquals(HConstants.EMPTY_BYTE_ARRAY, split.getStartRow());
                assertArrayEquals(Bytes.toBytes("row100"), split.getEndRow());
            } else if ("hbase_source_split_1".equals(split.splitId())) {
                foundRegion1Split = true;
                assertArrayEquals(Bytes.toBytes("row100"), split.getStartRow());
                assertArrayEquals(Bytes.toBytes("row150"), split.getEndRow());
            }
        }

        assertTrue(foundRegion0Split && foundRegion1Split);
    }

    @Test
    void testGetTableSplitsWithExactStartRowKeyMatch() throws IOException {
        byte[][] startKeys = {
            HConstants.EMPTY_BYTE_ARRAY,
            Bytes.toBytes("row100"),
            Bytes.toBytes("row200"),
            Bytes.toBytes("row300")
        };
        byte[][] endKeys = {
            Bytes.toBytes("row100"),
            Bytes.toBytes("row200"),
            Bytes.toBytes("row300"),
            HConstants.EMPTY_BYTE_ARRAY
        };

        when(regionLocator.getStartKeys()).thenReturn(startKeys);
        when(regionLocator.getEndKeys()).thenReturn(endKeys);
        when(hbaseParameters.getStartRowkey()).thenReturn("row100");

        Set<HbaseSourceSplit> splits = enumerator.getTableSplits();

        assertNotNull(splits);
        assertEquals(3, splits.size());

        boolean foundRegion1Split = false, foundRegion2Split = false, foundRegion3Split = false;
        for (HbaseSourceSplit split : splits) {
            if ("hbase_source_split_1".equals(split.splitId())) {
                foundRegion1Split = true;
                assertArrayEquals(Bytes.toBytes("row100"), split.getStartRow());
                assertArrayEquals(Bytes.toBytes("row200"), split.getEndRow());
            } else if ("hbase_source_split_2".equals(split.splitId())) {
                foundRegion2Split = true;
                assertArrayEquals(Bytes.toBytes("row200"), split.getStartRow());
                assertArrayEquals(Bytes.toBytes("row300"), split.getEndRow());
            } else if ("hbase_source_split_3".equals(split.splitId())) {
                foundRegion3Split = true;
                assertArrayEquals(Bytes.toBytes("row300"), split.getStartRow());
                assertArrayEquals(HConstants.EMPTY_BYTE_ARRAY, split.getEndRow());
            }
        }
        assertTrue(foundRegion1Split && foundRegion2Split && foundRegion3Split);
    }

    @Test
    void testGetTableSplitsWithExactEndRowKeyMatch() throws IOException {
        byte[][] startKeys = {
            HConstants.EMPTY_BYTE_ARRAY,
            Bytes.toBytes("row100"),
            Bytes.toBytes("row200"),
            Bytes.toBytes("row300")
        };
        byte[][] endKeys = {
            Bytes.toBytes("row100"),
            Bytes.toBytes("row200"),
            Bytes.toBytes("row300"),
            HConstants.EMPTY_BYTE_ARRAY
        };

        when(regionLocator.getStartKeys()).thenReturn(startKeys);
        when(regionLocator.getEndKeys()).thenReturn(endKeys);
        when(hbaseParameters.getEndRowkey()).thenReturn("row200");

        Set<HbaseSourceSplit> splits = enumerator.getTableSplits();

        assertNotNull(splits);
        assertEquals(2, splits.size());

        boolean foundRegion0Split = false, foundRegion1Split = false;
        for (HbaseSourceSplit split : splits) {
            if ("hbase_source_split_0".equals(split.splitId())) {
                foundRegion0Split = true;
                assertArrayEquals(HConstants.EMPTY_BYTE_ARRAY, split.getStartRow());
                assertArrayEquals(Bytes.toBytes("row100"), split.getEndRow());
            } else if ("hbase_source_split_1".equals(split.splitId())) {
                foundRegion1Split = true;
                assertArrayEquals(Bytes.toBytes("row100"), split.getStartRow());
                assertArrayEquals(Bytes.toBytes("row200"), split.getEndRow());
            }
        }
        assertTrue(foundRegion0Split && foundRegion1Split);
    }

    @Test
    void testGetTableSplitsWithExactRowKeyMatch() throws IOException {
        byte[][] startKeys = {
            HConstants.EMPTY_BYTE_ARRAY,
            Bytes.toBytes("row100"),
            Bytes.toBytes("row200"),
            Bytes.toBytes("row300")
        };
        byte[][] endKeys = {
            Bytes.toBytes("row100"),
            Bytes.toBytes("row200"),
            Bytes.toBytes("row300"),
            HConstants.EMPTY_BYTE_ARRAY
        };

        when(regionLocator.getStartKeys()).thenReturn(startKeys);
        when(regionLocator.getEndKeys()).thenReturn(endKeys);
        when(hbaseParameters.getStartRowkey()).thenReturn("row100");
        when(hbaseParameters.getEndRowkey()).thenReturn("row200");

        Set<HbaseSourceSplit> splits = enumerator.getTableSplits();

        assertNotNull(splits);
        assertEquals(1, splits.size());

        HbaseSourceSplit split = splits.iterator().next();
        assertEquals("hbase_source_split_1", split.splitId());
        assertArrayEquals(Bytes.toBytes("row100"), split.getStartRow());
        assertArrayEquals(Bytes.toBytes("row200"), split.getEndRow());
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.hbase.client;

import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;
import org.apache.seatunnel.connectors.seatunnel.hbase.source.HbaseSourceSplit;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HbaseClientTest {

    @Test
    void testBuildScanWithTimeRange() throws Exception {
        HbaseParameters hbaseParameters =
                HbaseParameters.builder().startTimestamp(1000L).endTimestamp(3000L).build();
        HbaseSourceSplit split = new HbaseSourceSplit(0, Bytes.toBytes("a"), Bytes.toBytes("b"));

        Scan scan = HbaseClient.buildScan(split, hbaseParameters, Arrays.asList("info:score"));

        TimeRange timeRange = scan.getTimeRange();
        assertEquals(1000L, timeRange.getMin());
        assertEquals(3000L, timeRange.getMax());
    }

    @Test
    void testBuildScanWithOnlyStartTimestamp() throws Exception {
        HbaseParameters hbaseParameters = HbaseParameters.builder().startTimestamp(1000L).build();
        HbaseSourceSplit split = new HbaseSourceSplit(0, Bytes.toBytes("a"), Bytes.toBytes("b"));

        Scan scan = HbaseClient.buildScan(split, hbaseParameters, Arrays.asList("info:score"));

        TimeRange timeRange = scan.getTimeRange();
        assertEquals(1000L, timeRange.getMin());
        assertEquals(Long.MAX_VALUE, timeRange.getMax());
    }

    @Test
    void testBuildScanWithOnlyEndTimestamp() throws Exception {
        HbaseParameters hbaseParameters = HbaseParameters.builder().endTimestamp(2000L).build();
        HbaseSourceSplit split = new HbaseSourceSplit(0, Bytes.toBytes("a"), Bytes.toBytes("b"));

        Scan scan = HbaseClient.buildScan(split, hbaseParameters, Arrays.asList("info:score"));

        TimeRange timeRange = scan.getTimeRange();
        assertEquals(0L, timeRange.getMin());
        assertEquals(2000L, timeRange.getMax());
    }

    @Test
    void testBuildScanWithInvalidTimeRange() {
        HbaseParameters hbaseParameters =
                HbaseParameters.builder().startTimestamp(3000L).endTimestamp(1000L).build();
        HbaseSourceSplit split = new HbaseSourceSplit(0, Bytes.toBytes("a"), Bytes.toBytes("b"));

        assertThrows(
                IllegalArgumentException.class,
                () -> HbaseClient.buildScan(split, hbaseParameters, Arrays.asList("info:score")));
    }

    @Test
    void testBuildScanWithNegativeMinTimestamp() {
        HbaseParameters hbaseParameters =
                HbaseParameters.builder().startTimestamp(-1L).endTimestamp(1000L).build();
        HbaseSourceSplit split = new HbaseSourceSplit(0, Bytes.toBytes("a"), Bytes.toBytes("b"));

        assertThrows(
                IllegalArgumentException.class,
                () -> HbaseClient.buildScan(split, hbaseParameters, Arrays.asList("info:score")));
    }

    @Test
    void testBuildScanWithNegativeMaxTimestamp() {
        HbaseParameters hbaseParameters = HbaseParameters.builder().endTimestamp(-1L).build();
        HbaseSourceSplit split = new HbaseSourceSplit(0, Bytes.toBytes("a"), Bytes.toBytes("b"));

        assertThrows(
                IllegalArgumentException.class,
                () -> HbaseClient.buildScan(split, hbaseParameters, Arrays.asList("info:score")));
    }

    @Test
    void testBuildScanWithEqualTimeRange() {
        HbaseParameters hbaseParameters =
                HbaseParameters.builder().startTimestamp(1000L).endTimestamp(1000L).build();
        HbaseSourceSplit split = new HbaseSourceSplit(0, Bytes.toBytes("a"), Bytes.toBytes("b"));

        assertThrows(
                IllegalArgumentException.class,
                () -> HbaseClient.buildScan(split, hbaseParameters, Arrays.asList("info:score")));
    }
}

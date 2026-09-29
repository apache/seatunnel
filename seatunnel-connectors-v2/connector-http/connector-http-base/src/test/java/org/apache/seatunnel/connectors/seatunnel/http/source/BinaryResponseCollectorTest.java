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

package org.apache.seatunnel.connectors.seatunnel.http.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class BinaryResponseCollectorTest {

    private static Collector<SeaTunnelRow> listCollector(List<SeaTunnelRow> list) {
        return new Collector<SeaTunnelRow>() {
            @Override
            public void collect(SeaTunnelRow record) {
                list.add(record);
            }

            @Override
            public Object getCheckpointLock() {
                return this;
            }
        };
    }

    @Test
    public void testSingleRowOutput() {
        byte[] data = new byte[1024];
        HttpResponse response = new HttpResponse(200, data, "attachment; filename=\"test.pdf\"");
        List<SeaTunnelRow> collected = new ArrayList<>();

        BinaryResponseCollector.collect(
                response, "http://example.com/file", 10 * 1024 * 1024L, listCollector(collected));

        Assertions.assertEquals(1, collected.size());
        SeaTunnelRow row = collected.get(0);
        Assertions.assertArrayEquals(data, (byte[]) row.getField(0));
        Assertions.assertEquals("test.pdf", row.getField(1));
        Assertions.assertEquals(0L, row.getField(2));
    }

    @Test
    public void testChunkedOutput() {
        byte[] data = new byte[25 * 1024 * 1024]; // 25MB
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        HttpResponse response = new HttpResponse(200, data, null);
        List<SeaTunnelRow> collected = new ArrayList<>();

        BinaryResponseCollector.collect(
                response,
                "http://example.com/files/big.zip",
                10 * 1024 * 1024L,
                listCollector(collected));

        Assertions.assertEquals(3, collected.size());

        // First chunk: 10MB
        SeaTunnelRow chunk0 = collected.get(0);
        Assertions.assertEquals(10 * 1024 * 1024, ((byte[]) chunk0.getField(0)).length);
        Assertions.assertEquals(0L, chunk0.getField(2));

        // Second chunk: 10MB
        SeaTunnelRow chunk1 = collected.get(1);
        Assertions.assertEquals(10 * 1024 * 1024, ((byte[]) chunk1.getField(0)).length);
        Assertions.assertEquals(1L, chunk1.getField(2));

        // Third chunk: 5MB
        SeaTunnelRow chunk2 = collected.get(2);
        Assertions.assertEquals(5 * 1024 * 1024, ((byte[]) chunk2.getField(0)).length);
        Assertions.assertEquals(2L, chunk2.getField(2));

        // All chunks have same filename
        String filename = (String) chunk0.getField(1);
        Assertions.assertEquals("big.zip", filename);
        Assertions.assertEquals(filename, chunk1.getField(1));
        Assertions.assertEquals(filename, chunk2.getField(1));

        // Verify data integrity: reassemble and compare
        byte[] reassembled = new byte[data.length];
        for (SeaTunnelRow row : collected) {
            byte[] chunk = (byte[]) row.getField(0);
            int partIndex = ((Long) row.getField(2)).intValue();
            System.arraycopy(chunk, 0, reassembled, partIndex * 10 * 1024 * 1024, chunk.length);
        }
        Assertions.assertArrayEquals(data, reassembled);
    }

    @Test
    public void testNullBodyBytes() {
        HttpResponse response = new HttpResponse(200, null, null);
        List<SeaTunnelRow> collected = new ArrayList<>();

        BinaryResponseCollector.collect(
                response, "http://example.com/file", 1024L, listCollector(collected));

        Assertions.assertEquals(0, collected.size());
    }

    @Test
    public void testEmptyBodyBytes() {
        HttpResponse response = new HttpResponse(200, new byte[0], null);
        List<SeaTunnelRow> collected = new ArrayList<>();

        BinaryResponseCollector.collect(
                response, "http://example.com/file", 1024L, listCollector(collected));

        Assertions.assertEquals(0, collected.size());
    }

    @Test
    public void testBinaryRowTypeStructure() {
        Assertions.assertEquals(3, BinaryResponseCollector.BINARY_ROW_TYPE.getTotalFields());
        Assertions.assertEquals("data", BinaryResponseCollector.BINARY_ROW_TYPE.getFieldName(0));
        Assertions.assertEquals(
                "relativePath", BinaryResponseCollector.BINARY_ROW_TYPE.getFieldName(1));
        Assertions.assertEquals(
                "partIndex", BinaryResponseCollector.BINARY_ROW_TYPE.getFieldName(2));
    }
}

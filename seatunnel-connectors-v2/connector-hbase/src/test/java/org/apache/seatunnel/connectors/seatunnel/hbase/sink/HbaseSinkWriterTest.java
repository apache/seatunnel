/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hbase.sink;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hbase.client.HbaseClient;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;
import org.apache.seatunnel.connectors.seatunnel.hbase.exception.HbaseConnectorException;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HbaseSinkWriterTest {

    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id", "name", "version"},
                    new SeaTunnelDataType[] {
                        BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.LONG_TYPE
                    });

    @Test
    void testBinaryRowkeyUsesRawBytes() throws Exception {
        HbaseParameters hbaseParameters =
                HbaseParameters.builder()
                        .familyNames(Collections.singletonMap("all_columns", "info"))
                        .build();
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"rowkey"},
                        new SeaTunnelDataType[] {PrimitiveByteArrayType.INSTANCE});
        byte[] rowkey = new byte[] {0x00, 0x01, 0x02, 0x03};
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {rowkey});
        HbaseClient hbaseClient = Mockito.mock(HbaseClient.class);

        try (MockedStatic<HbaseClient> mockedStatic = Mockito.mockStatic(HbaseClient.class)) {
            mockedStatic
                    .when(() -> HbaseClient.createInstance(Mockito.any(HbaseParameters.class)))
                    .thenReturn(hbaseClient);

            HbaseSinkWriter writer =
                    new HbaseSinkWriter(rowType, hbaseParameters, Arrays.asList(0), -1);
            writer.write(row);
        }

        ArgumentCaptor<Mutation> captor = ArgumentCaptor.forClass(Mutation.class);
        Mockito.verify(hbaseClient).mutate(captor.capture());
        assertInstanceOf(Put.class, captor.getValue());
        assertArrayEquals(rowkey, captor.getValue().getRow());
    }

    @Test
    void testInsertAndUpdateAfterRowsAreWrittenAsPut() throws Exception {
        HbaseClient hbaseClient = Mockito.mock(HbaseClient.class);
        HbaseSinkWriter writer = newWriter(hbaseClient, HbaseParameters.builder(), -1);

        writer.write(row(RowKind.INSERT, 1, "a", 10L));
        writer.write(row(RowKind.UPDATE_AFTER, 1, "b", 11L));

        ArgumentCaptor<Mutation> captor = ArgumentCaptor.forClass(Mutation.class);
        Mockito.verify(hbaseClient, Mockito.times(2)).mutate(captor.capture());
        for (Mutation mutation : captor.getAllValues()) {
            assertInstanceOf(Put.class, mutation);
            assertArrayEquals(Bytes.toBytes("1"), mutation.getRow());
        }
    }

    @Test
    void testDeleteRowIsWrittenAsDelete() throws Exception {
        HbaseClient hbaseClient = Mockito.mock(HbaseClient.class);
        HbaseSinkWriter writer = newWriter(hbaseClient, HbaseParameters.builder(), -1);

        writer.write(row(RowKind.DELETE, 7, "gone", 10L));

        ArgumentCaptor<Mutation> captor = ArgumentCaptor.forClass(Mutation.class);
        Mockito.verify(hbaseClient).mutate(captor.capture());
        assertInstanceOf(Delete.class, captor.getValue());
        assertArrayEquals(Bytes.toBytes("7"), captor.getValue().getRow());
        // Only the family this writer writes to is deleted, as a whole.
        assertEquals(Collections.singleton("info"), familiesOf(captor.getValue()));
        assertTrue(
                captor.getValue().getFamilyCellMap().values().stream()
                        .flatMap(List::stream)
                        .allMatch(cell -> CellUtil.cloneQualifier(cell).length == 0));
    }

    @Test
    void testDeleteCoversEveryMappedColumnFamily() throws Exception {
        HbaseClient hbaseClient = Mockito.mock(HbaseClient.class);
        Map<String, String> familyNames = new HashMap<>();
        familyNames.put("name", "cf1");
        familyNames.put("version", "cf2");
        HbaseSinkWriter writer =
                new HbaseSinkWriter(
                        ROW_TYPE,
                        HbaseParameters.builder().familyNames(familyNames).build(),
                        Collections.singletonList(0),
                        -1,
                        hbaseClient);

        writer.write(row(RowKind.DELETE, 7, "gone", 10L));

        ArgumentCaptor<Mutation> captor = ArgumentCaptor.forClass(Mutation.class);
        Mockito.verify(hbaseClient).mutate(captor.capture());
        assertEquals(new HashSet<>(Arrays.asList("cf1", "cf2")), familiesOf(captor.getValue()));
    }

    private static Set<String> familiesOf(Mutation mutation) {
        return mutation.getFamilyCellMap().keySet().stream()
                .map(Bytes::toString)
                .collect(Collectors.toSet());
    }

    @Test
    void testUpdateKeepingRowKeyOnlyWritesTheAfterImage() throws Exception {
        HbaseClient hbaseClient = Mockito.mock(HbaseClient.class);
        HbaseSinkWriter writer = newWriter(hbaseClient, HbaseParameters.builder(), -1);

        writer.write(row(RowKind.UPDATE_BEFORE, 7, "old", 10L));
        Mockito.verify(hbaseClient, Mockito.never()).mutate(Mockito.any());
        writer.write(row(RowKind.UPDATE_AFTER, 7, "new", 11L));

        ArgumentCaptor<Mutation> captor = ArgumentCaptor.forClass(Mutation.class);
        Mockito.verify(hbaseClient).mutate(captor.capture());
        assertInstanceOf(Put.class, captor.getValue());
        assertArrayEquals(Bytes.toBytes("7"), captor.getValue().getRow());
    }

    @Test
    void testUpdateMovingRowKeyDeletesTheOldRow() throws Exception {
        HbaseClient hbaseClient = Mockito.mock(HbaseClient.class);
        HbaseSinkWriter writer = newWriter(hbaseClient, HbaseParameters.builder(), -1);

        writer.write(row(RowKind.UPDATE_BEFORE, 7, "same", 10L));
        writer.write(row(RowKind.UPDATE_AFTER, 8, "same", 10L));

        ArgumentCaptor<Mutation> captor = ArgumentCaptor.forClass(Mutation.class);
        Mockito.verify(hbaseClient, Mockito.times(2)).mutate(captor.capture());
        List<Mutation> mutations = captor.getAllValues();
        assertInstanceOf(Delete.class, mutations.get(0));
        assertArrayEquals(Bytes.toBytes("7"), mutations.get(0).getRow());
        assertInstanceOf(Put.class, mutations.get(1));
        assertArrayEquals(Bytes.toBytes("8"), mutations.get(1).getRow());
    }

    @Test
    void testUnpairedUpdateBeforeIsDroppedByTheNextInsert() throws Exception {
        HbaseClient hbaseClient = Mockito.mock(HbaseClient.class);
        HbaseSinkWriter writer = newWriter(hbaseClient, HbaseParameters.builder(), -1);

        writer.write(row(RowKind.UPDATE_BEFORE, 7, "orphan", 10L));
        writer.write(row(RowKind.INSERT, 1, "a", 10L));
        writer.write(row(RowKind.UPDATE_AFTER, 9, "later", 10L));

        ArgumentCaptor<Mutation> captor = ArgumentCaptor.forClass(Mutation.class);
        Mockito.verify(hbaseClient, Mockito.times(2)).mutate(captor.capture());
        assertTrue(captor.getAllValues().stream().allMatch(m -> m instanceof Put));
    }

    @Test
    void testVersionColumnIsTheTimestampOfPutAndDelete() throws Exception {
        HbaseClient hbaseClient = Mockito.mock(HbaseClient.class);
        HbaseSinkWriter writer = newWriter(hbaseClient, HbaseParameters.builder(), 2);

        writer.write(row(RowKind.INSERT, 7, "here", 1233L));
        writer.write(row(RowKind.DELETE, 7, "gone", 1234L));

        ArgumentCaptor<Mutation> captor = ArgumentCaptor.forClass(Mutation.class);
        Mockito.verify(hbaseClient, Mockito.times(2)).mutate(captor.capture());
        assertInstanceOf(Put.class, captor.getAllValues().get(0));
        assertEquals(1233L, captor.getAllValues().get(0).getTimestamp());
        assertInstanceOf(Delete.class, captor.getAllValues().get(1));
        assertEquals(1234L, captor.getAllValues().get(1).getTimestamp());
    }

    @Test
    void testNullVersionColumnIsRejectedWithTheColumnName() {
        HbaseClient hbaseClient = Mockito.mock(HbaseClient.class);
        HbaseSinkWriter writer = newWriter(hbaseClient, HbaseParameters.builder(), 2);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {7, "gone", null});
        row.setRowKind(RowKind.DELETE);

        HbaseConnectorException exception =
                assertThrows(HbaseConnectorException.class, () -> writer.write(row));

        assertTrue(exception.getMessage().contains("version_column [version]"));
    }

    @Test
    void testWriterIsUnusableAfterAFailedFlush() throws Exception {
        HbaseClient hbaseClient = Mockito.mock(HbaseClient.class);
        Mockito.doThrow(new IOException("retries exhausted")).doNothing().when(hbaseClient).flush();
        HbaseSinkWriter writer = newWriter(hbaseClient, HbaseParameters.builder(), -1);
        writer.write(row(RowKind.INSERT, 1, "a", 10L));

        assertThrows(IOException.class, writer::prepareCommit);
        // The client dropped the failed rows, so a retry must not succeed with them missing.
        IOException retry = assertThrows(IOException.class, writer::prepareCommit);
        assertEquals("retries exhausted", retry.getCause().getMessage());
        assertThrows(IOException.class, () -> writer.write(row(RowKind.INSERT, 2, "b", 10L)));
        Mockito.verify(hbaseClient, Mockito.times(1)).flush();
    }

    @Test
    void testDeleteFollowsWalWriteSetting() throws Exception {
        HbaseClient hbaseClient = Mockito.mock(HbaseClient.class);
        HbaseSinkWriter writer =
                newWriter(hbaseClient, HbaseParameters.builder().walWrite(false), -1);

        writer.write(row(RowKind.DELETE, 7, "gone", 10L));

        ArgumentCaptor<Mutation> captor = ArgumentCaptor.forClass(Mutation.class);
        Mockito.verify(hbaseClient).mutate(captor.capture());
        assertEquals(Durability.SKIP_WAL, captor.getValue().getDurability());
    }

    @Test
    void testPrepareCommitFlushesBufferedMutations() throws Exception {
        HbaseClient hbaseClient = Mockito.mock(HbaseClient.class);
        HbaseSinkWriter writer = newWriter(hbaseClient, HbaseParameters.builder(), -1);

        writer.write(row(RowKind.INSERT, 1, "a", 10L));
        writer.prepareCommit();

        Mockito.verify(hbaseClient).flush();
    }

    private static HbaseSinkWriter newWriter(
            HbaseClient hbaseClient,
            HbaseParameters.HbaseParametersBuilder parameters,
            int versionColumnIndex) {
        return new HbaseSinkWriter(
                ROW_TYPE,
                parameters.familyNames(Collections.singletonMap("all_columns", "info")).build(),
                Collections.singletonList(0),
                versionColumnIndex,
                hbaseClient);
    }

    private static SeaTunnelRow row(RowKind kind, int id, String name, long version) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, name, version});
        row.setRowKind(kind);
        return row;
    }
}

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

import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hbase.client.HbaseClient;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;

import org.apache.hadoop.hbase.client.Put;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class HbaseSinkWriterTest {

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

        ArgumentCaptor<Put> putCaptor = ArgumentCaptor.forClass(Put.class);
        Mockito.verify(hbaseClient).mutate(putCaptor.capture());
        assertArrayEquals(rowkey, putCaptor.getValue().getRow());
    }
}

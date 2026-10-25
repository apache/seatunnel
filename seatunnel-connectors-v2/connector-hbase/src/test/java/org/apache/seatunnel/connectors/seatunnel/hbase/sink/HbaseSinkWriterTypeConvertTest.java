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

package org.apache.seatunnel.connectors.seatunnel.hbase.sink;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hbase.client.HbaseClient;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;
import org.apache.seatunnel.connectors.seatunnel.hbase.format.HBaseDeserializationFormat;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class HbaseSinkWriterTypeConvertTest {

    @Test
    public void testWriteAndDeserializeTemporalAndDecimalTypes() throws Exception {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name", "c_decimal", "c_date", "c_time", "c_timestamp"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE,
                            new DecimalType(10, 2),
                            LocalTimeType.LOCAL_DATE_TYPE,
                            LocalTimeType.LOCAL_TIME_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });

        HbaseClient hbaseClient = mock(HbaseClient.class);
        HbaseParameters parameters =
                HbaseParameters.builder()
                        .familyNames(Collections.singletonMap("all_columns", "info"))
                        .build();

        HbaseSinkWriter writer =
                new HbaseSinkWriter(
                        rowType, parameters, Collections.singletonList(0), -1, hbaseClient);

        SeaTunnelRow row =
                new SeaTunnelRow(
                        new Object[] {
                            "row1",
                            new BigDecimal("999999.90"),
                            LocalDate.parse("2012-12-21"),
                            LocalTime.parse("12:34:56"),
                            LocalDateTime.parse("2012-12-21T12:34:56")
                        });

        writer.write(row);

        ArgumentCaptor<Put> putCaptor = ArgumentCaptor.forClass(Put.class);
        verify(hbaseClient).mutate(putCaptor.capture());
        Put put = putCaptor.getValue();

        assertArrayEquals(Bytes.toBytes("row1"), put.getRow());

        byte[] family = Bytes.toBytes("info");
        byte[] decimalBytes = getValue(put, family, "c_decimal");
        byte[] dateBytes = getValue(put, family, "c_date");
        byte[] timeBytes = getValue(put, family, "c_time");
        byte[] timestampBytes = getValue(put, family, "c_timestamp");

        assertEquals("999999.90", Bytes.toString(decimalBytes));
        assertEquals("2012-12-21", Bytes.toString(dateBytes));
        assertEquals("12:34:56", Bytes.toString(timeBytes));
        assertEquals("2012-12-21 12:34:56", Bytes.toString(timestampBytes));

        HBaseDeserializationFormat deserializationFormat = new HBaseDeserializationFormat();
        SeaTunnelRowType deserializeRowType =
                new SeaTunnelRowType(
                        new String[] {"c_decimal", "c_date", "c_time", "c_timestamp"},
                        new SeaTunnelDataType[] {
                            new DecimalType(10, 2),
                            LocalTimeType.LOCAL_DATE_TYPE,
                            LocalTimeType.LOCAL_TIME_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });

        SeaTunnelRow deserialized =
                deserializationFormat.deserialize(
                        new byte[][] {decimalBytes, dateBytes, timeBytes, timestampBytes},
                        deserializeRowType);

        assertEquals(new BigDecimal("999999.90"), deserialized.getField(0));
        assertEquals(LocalDate.parse("2012-12-21"), deserialized.getField(1));
        assertEquals(LocalTime.parse("12:34:56"), deserialized.getField(2));
        assertEquals(LocalDateTime.parse("2012-12-21T12:34:56"), deserialized.getField(3));
    }

    private static byte[] getValue(Put put, byte[] family, String qualifier) {
        List<Cell> cells = put.get(family, Bytes.toBytes(qualifier));
        assertNotNull(cells);
        assertFalse(cells.isEmpty());
        return CellUtil.cloneValue(cells.get(0));
    }
}

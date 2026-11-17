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

package org.apache.seatunnel.connectors.doris.source.reader;

import org.apache.seatunnel.shade.org.apache.arrow.memory.RootAllocator;
import org.apache.seatunnel.shade.org.apache.arrow.vector.VarCharVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.seatunnel.shade.org.apache.arrow.vector.complex.ListVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.seatunnel.shade.org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.seatunnel.shade.org.apache.arrow.vector.util.Text;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.doris.datatype.DorisTypeConverterV2;
import org.apache.seatunnel.connectors.seatunnel.common.source.arrow.reader.ArrowToSeatunnelRowReader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;

public class DorisLargeintReadTest {

    private static final String MAX_LARGEINT = "170141183460469231731687303715884105727";
    private static final String MIN_LARGEINT = "-170141183460469231731687303715884105728";
    private static final String DIGITS_21 = "123456789012345678901";

    @Test
    public void testLargeintTypeHoldsWholeRange() {
        DecimalType decimalType = (DecimalType) convert("largeint", "LARGEINT").getDataType();
        for (String value : new String[] {DIGITS_21, MAX_LARGEINT, MIN_LARGEINT}) {
            BigDecimal decimal = new BigDecimal(value);
            Assertions.assertTrue(
                    decimal.precision() <= decimalType.getPrecision(),
                    value + " does not fit " + decimalType);
            Assertions.assertEquals(decimalType.getScale(), decimal.scale());
        }
    }

    @Test
    public void testReadLargeintFromArrowUtf8() throws Exception {
        // Doris BE sends LARGEINT as Utf8 and ARRAY<LARGEINT> as a list of Utf8
        byte[] arrow;
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                VarCharVector value = new VarCharVector("v", allocator);
                ListVector values = ListVector.empty("arr", allocator)) {
            String[] rows = {DIGITS_21, MAX_LARGEINT, MIN_LARGEINT};
            value.allocateNew(rows.length + 1);
            for (int i = 0; i < rows.length; i++) {
                value.setSafe(i, rows[i].getBytes(StandardCharsets.UTF_8));
            }
            value.setNull(rows.length);
            value.setValueCount(rows.length + 1);

            UnionListWriter writer = values.getWriter();
            writer.setPosition(0);
            writer.startList();
            writer.writeVarChar(new Text(MAX_LARGEINT));
            writer.writeVarChar(new Text(MIN_LARGEINT));
            writer.endList();
            for (int i = 1; i <= rows.length; i++) {
                writer.setPosition(i);
                writer.startList();
                writer.endList();
            }
            writer.setValueCount(rows.length + 1);

            try (VectorSchemaRoot root = VectorSchemaRoot.of(value, values);
                    ByteArrayOutputStream output = new ByteArrayOutputStream();
                    ArrowStreamWriter streamWriter =
                            new ArrowStreamWriter(root, null, Channels.newChannel(output))) {
                streamWriter.start();
                streamWriter.writeBatch();
                streamWriter.end();
                arrow = output.toByteArray();
            }
        }

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"v", "arr"},
                        new SeaTunnelDataType<?>[] {
                            convert("largeint", "LARGEINT").getDataType(),
                            convert("array<largeint>", "ARRAY").getDataType()
                        });
        try (ArrowToSeatunnelRowReader reader =
                new ArrowToSeatunnelRowReader(arrow, rowType).readArrow()) {
            SeaTunnelRow row = reader.next();
            Assertions.assertEquals(new BigDecimal(DIGITS_21), row.getField(0));
            Assertions.assertArrayEquals(
                    new BigDecimal[] {new BigDecimal(MAX_LARGEINT), new BigDecimal(MIN_LARGEINT)},
                    (BigDecimal[]) row.getField(1));
            Assertions.assertEquals(new BigDecimal(MAX_LARGEINT), reader.next().getField(0));
            Assertions.assertEquals(new BigDecimal(MIN_LARGEINT), reader.next().getField(0));
            Assertions.assertNull(reader.next().getField(0));
            Assertions.assertFalse(reader.hasNext());
        }
    }

    private static Column convert(String columnType, String dataType) {
        return DorisTypeConverterV2.INSTANCE.convert(
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType(columnType)
                        .dataType(dataType)
                        .build());
    }
}

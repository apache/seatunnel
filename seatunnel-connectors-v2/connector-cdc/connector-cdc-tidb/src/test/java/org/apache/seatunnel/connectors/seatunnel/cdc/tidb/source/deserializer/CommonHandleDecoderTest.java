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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.deserializer;

import org.junit.jupiter.api.Test;
import org.tikv.common.codec.Codec.IntegerCodec;
import org.tikv.common.codec.CodecDataOutput;
import org.tikv.common.meta.CIStr;
import org.tikv.common.meta.IndexType;
import org.tikv.common.meta.SchemaState;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiIndexColumn;
import org.tikv.common.meta.TiIndexInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.types.DataType.EncodeType;
import org.tikv.common.types.DecimalType;
import org.tikv.common.types.StringType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.tikv.common.codec.TableCodec.decodeObjects;
import static org.tikv.common.codec.TableCodec.encodeRow;

class CommonHandleDecoderTest {

    @Test
    void restoresDecimalPrimaryKeyFromCommonHandleRecordKey() throws Exception {
        TiColumnInfo primaryKey = new TiColumnInfo(1, "id", 0, new DecimalType(10, 0), true);
        TiColumnInfo name = new TiColumnInfo(2, "name", 1, StringType.VARCHAR, false);
        TiTableInfo tableInfo = tableInfo(primaryKey, name);
        Object[] values =
                decodeObjects(
                        encodeRow(
                                Collections.singletonList(name),
                                new Object[] {"Alice"},
                                false,
                                false),
                        null,
                        tableInfo);

        assertNull(values[0]);
        assertEquals("Alice", values[1]);

        CommonHandleDecoder.restorePrimaryKeyColumns(
                recordKey(tableInfo.getId(), primaryKey, new BigDecimal("1001")),
                values,
                tableInfo);

        assertEquals(new BigDecimal("1001"), new BigDecimal(values[0].toString()));
        assertEquals("Alice", values[1]);
    }

    private TiTableInfo tableInfo(TiColumnInfo primaryKey, TiColumnInfo name) {
        TiIndexInfo primaryIndex =
                new TiIndexInfo(
                        1,
                        CIStr.newCIStr("primary"),
                        CIStr.newCIStr("people"),
                        Collections.singletonList(
                                new TiIndexColumn(
                                        CIStr.newCIStr("id"), primaryKey.getOffset(), -1)),
                        true,
                        true,
                        SchemaState.StatePublic.getStateCode(),
                        "",
                        IndexType.IndexTypeBtree.getTypeCode(),
                        false);
        return new TiTableInfo(
                101,
                CIStr.newCIStr("people"),
                "utf8mb4",
                "utf8mb4_bin",
                false,
                Arrays.asList(primaryKey, name),
                Collections.singletonList(primaryIndex),
                "",
                0,
                2,
                1,
                0,
                null,
                null,
                null,
                0,
                0,
                0,
                null);
    }

    private byte[] recordKey(long tableId, TiColumnInfo primaryKey, BigDecimal primaryKeyValue) {
        CodecDataOutput output = new CodecDataOutput();
        output.writeByte('t');
        IntegerCodec.writeLong(output, tableId);
        output.writeByte('_');
        output.writeByte('r');
        primaryKey.getType().encode(output, EncodeType.KEY, primaryKeyValue);
        return output.toBytes();
    }
}

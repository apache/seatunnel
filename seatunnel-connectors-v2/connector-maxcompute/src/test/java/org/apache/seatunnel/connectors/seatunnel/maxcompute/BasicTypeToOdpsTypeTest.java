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

package org.apache.seatunnel.connectors.seatunnel.maxcompute;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.util.MaxcomputeTypeMapper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import lombok.SneakyThrows;

public class BasicTypeToOdpsTypeTest {

    private static void testType(
            String fieldName,
            SeaTunnelDataType<?> seaTunnelDataType,
            OdpsType odpsType,
            Object object) {
        SeaTunnelRowType typeInfo =
                new SeaTunnelRowType(
                        new String[] {fieldName}, new SeaTunnelDataType<?>[] {seaTunnelDataType});

        ArrayRecord record = new ArrayRecord(new Column[] {new Column(fieldName, odpsType)});
        record.set(fieldName, object);

        TableSchema tableSchema = new TableSchema();
        for (Column column : record.getColumns()) {
            tableSchema.addColumn(column);
        }

        SeaTunnelRow seaTunnelRow = MaxcomputeTypeMapper.getSeaTunnelRowData(record, typeInfo);
        Record tRecord =
                MaxcomputeTypeMapper.getMaxcomputeRowData(seaTunnelRow, tableSchema, typeInfo);

        for (int i = 0; i < tRecord.getColumns().length; i++) {
            Assertions.assertEquals(record.get(i), tRecord.get(i));
        }
    }

    @SneakyThrows
    @Test
    void testSTRING_TYPE_2_STRING() {
        testType("STRING_TYPE_2_STRING", BasicType.STRING_TYPE, OdpsType.STRING, "hello");
    }

    @SneakyThrows
    @Test
    void testBOOLEAN_TYPE_2_BOOLEAN() {
        testType("BOOLEAN_TYPE_2_BOOLEAN", BasicType.BOOLEAN_TYPE, OdpsType.BOOLEAN, Boolean.TRUE);
    }

    @SneakyThrows
    @Test
    void testSHORT_TYPE_2_SMALLINT() {
        testType("SHORT_TYPE_2_SMALLINT", BasicType.SHORT_TYPE, OdpsType.SMALLINT, Short.MAX_VALUE);
    }

    @SneakyThrows
    @Test
    void testLONG_TYPE_2_BIGINT() {
        testType("LONG_TYPE_2_BIGINT", BasicType.LONG_TYPE, OdpsType.BIGINT, Long.MAX_VALUE);
    }

    @SneakyThrows
    @Test
    void testFLOAT_TYPE_2_FLOAT_TYPE() {
        testType("FLOAT_TYPE_2_FLOAT_TYPE", BasicType.FLOAT_TYPE, OdpsType.FLOAT, Float.MAX_VALUE);
    }

    @SneakyThrows
    @Test
    void testDOUBLE_TYPE_2_DOUBLE() {
        testType("DOUBLE_TYPE_2_DOUBLE", BasicType.DOUBLE_TYPE, OdpsType.DOUBLE, Double.MAX_VALUE);
    }

    @SneakyThrows
    @Test
    void testVOID_TYPE_2_VOID() {
        testType("VOID_TYPE_2_VOID", BasicType.VOID_TYPE, OdpsType.VOID, null);
    }
}

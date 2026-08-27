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
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.util.FormatterContext;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.util.MaxcomputeTypeMapper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import lombok.SneakyThrows;

import java.sql.Timestamp;
import java.time.LocalDate;

public class BasicTypeToOdpsTypeTest {
    public static FormatterContext defaultFormatterContext =
            new FormatterContext("yyyy-MM-dd HH:mm:ss");

    public static FormatterContext customFormatterContext =
            new FormatterContext("yyyy-MM-dd HH:mm:ss.SSSSSS");

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
                MaxcomputeTypeMapper.getMaxcomputeRowData(
                        new ArrayRecord(tableSchema),
                        seaTunnelRow,
                        tableSchema,
                        typeInfo,
                        defaultFormatterContext);

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

    @SneakyThrows
    @Test
    void testDATE_TYPE_2_DATE() {
        testType("DATE_TYPE_2_DATE", LocalTimeType.LOCAL_DATE_TYPE, OdpsType.DATE, LocalDate.now());
    }

    @SneakyThrows
    @Test
    void testLOCAL_DATETIME_2_STRING() {
        testTypeWithDifferentInputAndOutput(
                "LOCAL_DATETIME_2_STRING",
                OdpsType.TIMESTAMP,
                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                OdpsType.STRING,
                Timestamp.valueOf("2025-01-01 00:00:00"),
                "2025-01-01 00:00:00",
                defaultFormatterContext);

        testTypeWithDifferentInputAndOutput(
                "LOCAL_DATETIME_2_STRING",
                OdpsType.TIMESTAMP,
                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                OdpsType.STRING,
                Timestamp.valueOf("2025-01-01 00:00:00"),
                "2025-01-01 00:00:00.000000",
                customFormatterContext);
    }

    private static void testTypeWithDifferentInputAndOutput(
            String fieldName,
            OdpsType inputOdpsType,
            SeaTunnelDataType<?> seaTunnelDataType,
            OdpsType outputOdpsType,
            Object inputObject,
            Object expectedObject,
            FormatterContext formatterContext) {
        Column inputColumn = new Column(fieldName, inputOdpsType);
        ArrayRecord inputRecord = new ArrayRecord(new Column[] {inputColumn});
        inputRecord.set(fieldName, inputObject);

        SeaTunnelRowType typeInfo =
                new SeaTunnelRowType(
                        new String[] {fieldName}, new SeaTunnelDataType<?>[] {seaTunnelDataType});

        SeaTunnelRow seaTunnelRow = MaxcomputeTypeMapper.getSeaTunnelRowData(inputRecord, typeInfo);

        Column outputColumn = new Column(fieldName, outputOdpsType);
        TableSchema outputSchema = new TableSchema();
        outputSchema.addColumn(outputColumn);

        Record finalOutputRecord =
                MaxcomputeTypeMapper.getMaxcomputeRowData(
                        new ArrayRecord(outputSchema),
                        seaTunnelRow,
                        outputSchema,
                        typeInfo,
                        formatterContext);

        Assertions.assertEquals(expectedObject, finalOutputRecord.get(fieldName));
    }
}

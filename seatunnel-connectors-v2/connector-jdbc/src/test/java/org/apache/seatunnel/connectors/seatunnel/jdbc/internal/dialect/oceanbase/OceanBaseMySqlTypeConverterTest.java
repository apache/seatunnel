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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oceanbase;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.LocalTimeType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link OceanBaseMySqlTypeConverter} verifying the NTZ/LTZ timestamp split
 * introduced by the fix for https://github.com/apache/seatunnel/issues/10685.
 */
public class OceanBaseMySqlTypeConverterTest {

    @Test
    public void testConvertDatetimeIsNtz() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("DATETIME")
                        .dataType("DATETIME")
                        .build();
        Column column = OceanBaseMySqlTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        // DATETIME is NTZ → must map to LOCAL_DATE_TIME_TYPE
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
    }

    @Test
    public void testConvertTimestampIsLtz() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("TIMESTAMP")
                        .dataType("TIMESTAMP")
                        .build();
        Column column = OceanBaseMySqlTypeConverter.INSTANCE.convert(typeDefine);
        Assertions.assertEquals(typeDefine.getName(), column.getName());
        // TIMESTAMP is LTZ → must map to OFFSET_DATE_TIME_TYPE
        Assertions.assertEquals(LocalTimeType.OFFSET_DATE_TIME_TYPE, column.getDataType());
    }

    @Test
    public void testReconvertDatetime() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .build();
        BasicTypeDefine<?> typeDefine = OceanBaseMySqlTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        // LOCAL_DATE_TIME_TYPE (NTZ) → DATETIME
        Assertions.assertEquals(
                OceanBaseMySqlTypeConverter.MYSQL_DATETIME, typeDefine.getColumnType());
        Assertions.assertEquals(
                OceanBaseMySqlTypeConverter.MYSQL_DATETIME, typeDefine.getDataType());
    }

    @Test
    public void testReconvertDatetimeTz() {
        Column column =
                PhysicalColumn.builder()
                        .name("test")
                        .dataType(LocalTimeType.OFFSET_DATE_TIME_TYPE)
                        .build();
        BasicTypeDefine<?> typeDefine = OceanBaseMySqlTypeConverter.INSTANCE.reconvert(column);
        Assertions.assertEquals(column.getName(), typeDefine.getName());
        // OFFSET_DATE_TIME_TYPE (LTZ) → TIMESTAMP
        Assertions.assertEquals(
                OceanBaseMySqlTypeConverter.MYSQL_TIMESTAMP, typeDefine.getColumnType());
        Assertions.assertEquals(
                OceanBaseMySqlTypeConverter.MYSQL_TIMESTAMP, typeDefine.getDataType());
    }
}

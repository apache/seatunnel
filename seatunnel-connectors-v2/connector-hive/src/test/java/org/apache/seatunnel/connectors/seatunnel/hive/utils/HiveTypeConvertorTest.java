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

package org.apache.seatunnel.connectors.seatunnel.hive.utils;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.hive.catalog.HiveTypeConvertor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HiveTypeConvertorTest {

    @Test
    void covertHiveTypeToSeaTunnelType() {
        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> HiveTypeConvertor.covertHiveTypeToSeaTunnelType("test", "char"));
        assertEquals(
                "ErrorCode:[COMMON-16], ErrorDescription:['Hive' source unsupported convert type 'char' of 'test' to SeaTunnel data type.]",
                exception.getMessage());
    }

    @Test
    void convertHiveStructType() {
        SeaTunnelDataType<?> structType =
                HiveTypeConvertor.covertHiveTypeToSeaTunnelType(
                        "structType", "struct<country:String,city:String>");
        assertEquals(SqlType.ROW, structType.getSqlType());
        SeaTunnelRowType seaTunnelRowType = (SeaTunnelRowType) structType;
        assertEquals(BasicType.STRING_TYPE, seaTunnelRowType.getFieldType(0));
        assertEquals(BasicType.STRING_TYPE, seaTunnelRowType.getFieldType(0));
    }
}

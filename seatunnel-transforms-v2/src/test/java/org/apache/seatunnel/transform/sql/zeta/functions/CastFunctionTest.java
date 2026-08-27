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

package org.apache.seatunnel.transform.sql.zeta.functions;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.sql.SQLEngine;
import org.apache.seatunnel.transform.sql.SQLEngineFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CastFunctionTest {

    @Test
    public void testCastFunction() {

        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"f1"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        String f1 = "1";
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {f1});

        sqlEngine.init(
                "test",
                null,
                rowType,
                "select f1, cast(f1 as TINYINT) as f2, cast(f1 as SMALLINT) as f3 from test");
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, rowType).get(0);
        Object f1Object = outRow.getField(0);
        Object f2Object = outRow.getField(1);
        Object f3Object = outRow.getField(2);
        Assertions.assertEquals("1", f1Object);
        Assertions.assertEquals(Byte.parseByte("1"), f2Object);
        Assertions.assertEquals(Short.parseShort("1"), f3Object);
    }

    @Test
    public void testCastFunctionWithNullNestedField() {
        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"user"},
                        new SeaTunnelDataType[] {
                            new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE)
                        });

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {null});

        sqlEngine.init("test", null, rowType, "select user.address as address from test");

        SeaTunnelRowType outRowType = sqlEngine.typeMapping(null);

        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, outRowType).get(0);

        Object addressField = outRow.getField(0);
        Assertions.assertNull(
                addressField,
                "When casting nested field where intermediate value is null, result should be null");
    }

    @Test
    public void testCastFunctionWithNestedField() {
        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);

        MapType<String, String> mapType =
                new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(new String[] {"user"}, new SeaTunnelDataType[] {mapType});

        java.util.Map<String, String> userData = new java.util.HashMap<>();
        userData.put("address", "123 Main St");
        userData.put("age", "25");
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {userData});

        sqlEngine.init(
                "test",
                null,
                rowType,
                "select user.address as address, cast(user.age as INT) as age from test");

        SeaTunnelRowType outRowType = sqlEngine.typeMapping(null);
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, outRowType).get(0);

        Assertions.assertEquals("123 Main St", outRow.getField(0));
        Assertions.assertEquals(25, outRow.getField(1));
    }

    @Test
    public void testCastFunctionWithNormalValues() {
        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"str_field", "int_field"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {"42", 100});

        sqlEngine.init(
                "test",
                null,
                rowType,
                "select cast(str_field as INT) as cast_to_int, cast(int_field as STRING) as cast_to_str from test");

        SeaTunnelRowType outRowType = sqlEngine.typeMapping(null);
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, outRowType).get(0);

        Assertions.assertEquals(42, outRow.getField(0));
        Assertions.assertEquals("100", outRow.getField(1));
    }

    @Test
    public void testCastWithNestedFunctions() {
        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"text", "int_field"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {"12345", 456});

        String sql =
                "select CAST(LEFT(text, 2) AS INT) as cast_left,"
                        + " CONCAT_WS('-', LEFT(text, 3), CAST(int_field AS STRING)) as concat_ws_cast,"
                        + " CAST(CONCAT_WS('', LEFT(text, 1), RIGHT(text, 1)) AS INT) as cast_concat_ws"
                        + " from test";

        sqlEngine.init("test", null, rowType, sql);

        SeaTunnelRowType outRowType = sqlEngine.typeMapping(null);
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, outRowType).get(0);

        Assertions.assertEquals(12, outRow.getField(0));
        Assertions.assertEquals("123-456", outRow.getField(1));
        Assertions.assertEquals(15, outRow.getField(2));
    }

    @Test
    public void testNestedRowFieldAccess() {
        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);

        SeaTunnelRowType userRowType =
                new SeaTunnelRowType(
                        new String[] {"street", "city"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(new String[] {"user"}, new SeaTunnelDataType[] {userRowType});

        SeaTunnelRow innerRow = new SeaTunnelRow(new Object[] {"123 Main St", "New York"});
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {innerRow});

        sqlEngine.init(
                "test", null, rowType, "select user.street as street, user.city as city from test");

        SeaTunnelRowType outRowType = sqlEngine.typeMapping(null);
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, outRowType).get(0);

        Assertions.assertEquals("123 Main St", outRow.getField(0));
        Assertions.assertEquals("New York", outRow.getField(1));
    }

    @Test
    public void testMultiLevelNestedRowFieldAccess() {
        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);

        SeaTunnelRowType addressRowType =
                new SeaTunnelRowType(
                        new String[] {"street", "zipcode"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});

        SeaTunnelRowType userRowType =
                new SeaTunnelRowType(
                        new String[] {"name", "address"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, addressRowType});

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(new String[] {"user"}, new SeaTunnelDataType[] {userRowType});

        SeaTunnelRow addressRow = new SeaTunnelRow(new Object[] {"123 Main St", "10001"});
        SeaTunnelRow userRow = new SeaTunnelRow(new Object[] {"John Doe", addressRow});
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {userRow});

        sqlEngine.init(
                "test",
                null,
                rowType,
                "select user.address.street as street, user.name as name from test");

        SeaTunnelRowType outRowType = sqlEngine.typeMapping(null);
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, outRowType).get(0);

        Assertions.assertEquals("123 Main St", outRow.getField(0));
        Assertions.assertEquals("John Doe", outRow.getField(1));
    }

    @Test
    public void testMapFieldNormalAccess() {
        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"user"},
                        new SeaTunnelDataType[] {
                            new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE)
                        });

        java.util.Map<String, String> userData = new java.util.HashMap<>();
        userData.put("name", "John Doe");
        userData.put("email", "john@example.com");
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {userData});

        sqlEngine.init(
                "test", null, rowType, "select user.name as name, user.email as email from test");

        SeaTunnelRowType outRowType = sqlEngine.typeMapping(null);
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, outRowType).get(0);

        Assertions.assertEquals("John Doe", outRow.getField(0));
        Assertions.assertEquals("john@example.com", outRow.getField(1));
    }

    @Test
    public void testNestedFieldWithNullIntermediateValue() {
        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);

        SeaTunnelRowType addressRowType =
                new SeaTunnelRowType(
                        new String[] {"street", "zipcode"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});

        SeaTunnelRowType userRowType =
                new SeaTunnelRowType(
                        new String[] {"name", "address"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, addressRowType});

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(new String[] {"user"}, new SeaTunnelDataType[] {userRowType});

        SeaTunnelRow addressRow1 = new SeaTunnelRow(new Object[] {"beijing", "10001"});
        SeaTunnelRow userRow1 = new SeaTunnelRow(new Object[] {"zhangsan", addressRow1});
        SeaTunnelRow inputRow1 = new SeaTunnelRow(new Object[] {userRow1});

        sqlEngine.init(
                "test",
                null,
                rowType,
                "select user.address.street as street, user.name as name from test");

        SeaTunnelRowType outRowType = sqlEngine.typeMapping(null);
        SeaTunnelRow outRow1 = sqlEngine.transformBySQL(inputRow1, outRowType).get(0);

        Assertions.assertEquals("beijing", outRow1.getField(0));
        Assertions.assertEquals("zhangsan", outRow1.getField(1));

        SeaTunnelRow userRow2 = new SeaTunnelRow(new Object[] {"lisi", null});
        SeaTunnelRow inputRow2 = new SeaTunnelRow(new Object[] {userRow2});

        SeaTunnelRow outRow2 = sqlEngine.transformBySQL(inputRow2, outRowType).get(0);

        Assertions.assertNull(
                outRow2.getField(0),
                "When accessing nested field where intermediate value is null, result should be null");
        Assertions.assertEquals("lisi", outRow2.getField(1));
    }
}

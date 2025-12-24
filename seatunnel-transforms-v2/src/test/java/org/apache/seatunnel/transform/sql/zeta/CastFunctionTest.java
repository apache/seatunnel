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

package org.apache.seatunnel.transform.sql.zeta;

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

        // Create a map with nested data
        MapType<String, String> mapType =
                new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(new String[] {"user"}, new SeaTunnelDataType[] {mapType});

        // Create input data with nested fields
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

        // Check nested field access
        Assertions.assertEquals("123 Main St", outRow.getField(0)); // Direct access
        Assertions.assertEquals(25, outRow.getField(1)); // Cast from nested field
    }

    @Test
    public void testCastFunctionWithNormalValues() {
        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"str_field", "int_field"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {"123", 456});

        sqlEngine.init(
                "test",
                null,
                rowType,
                "select str_field, cast(str_field as INT) as int_from_str, "
                        + "int_field, cast(int_field as STRING) as str_from_int from test");

        SeaTunnelRowType outRowType = sqlEngine.typeMapping(null);
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, outRowType).get(0);

        // Original values should remain unchanged
        Assertions.assertEquals("123", outRow.getField(0));
        Assertions.assertEquals(456, outRow.getField(2));

        // Cast conversions
        Assertions.assertEquals(123, outRow.getField(1)); // String to Int
        Assertions.assertEquals("456", outRow.getField(3)); // Int to String
    }

    @Test
    public void testNormalNestedRowFieldAccess() {
        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);

        // Create nested row type structure
        SeaTunnelRowType innerRowType =
                new SeaTunnelRowType(
                        new String[] {"street", "city"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(new String[] {"user"}, new SeaTunnelDataType[] {innerRowType});

        // Create nested row data
        SeaTunnelRow innerRow = new SeaTunnelRow(new Object[] {"123 Main St", "New York"});
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {innerRow});

        sqlEngine.init(
                "test", null, rowType, "select user.street as street, user.city as city from test");

        SeaTunnelRowType outRowType = sqlEngine.typeMapping(null);
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, outRowType).get(0);

        // Verify normal nested field access (testing lines 343-345 in ZetaSQLFunction)
        Assertions.assertEquals("123 Main St", outRow.getField(0));
        Assertions.assertEquals("New York", outRow.getField(1));
    }

    @Test
    public void testMultiLevelNestedRowFieldAccess() {
        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);

        // Create multi-level nested row type structure
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

        // Create multi-level nested row data
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

        // Verify multi-level nested field access (testing lines 343-345 in ZetaSQLFunction)
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

        // Create map data with actual values (testing normal access scenario)
        java.util.Map<String, String> userData = new java.util.HashMap<>();
        userData.put("name", "John Doe");
        userData.put("email", "john@example.com");
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {userData});

        sqlEngine.init(
                "test", null, rowType, "select user.name as name, user.email as email from test");

        SeaTunnelRowType outRowType = sqlEngine.typeMapping(null);
        SeaTunnelRow outRow = sqlEngine.transformBySQL(inputRow, outRowType).get(0);

        // Verify map field access (testing normal access scenario for lines 343-345)
        Assertions.assertEquals("John Doe", outRow.getField(0));
        Assertions.assertEquals("john@example.com", outRow.getField(1));
    }

    @Test
    public void testNestedFieldWithNullIntermediateValue() {
        SQLEngine sqlEngine = SQLEngineFactory.getSQLEngine(SQLEngineFactory.EngineType.ZETA);

        // Create multi-level nested row type structure: user -> address -> street
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

        // Test case 1: Normal nested access (user.address.street should return "beijing")
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

        // Verify normal nested field access
        Assertions.assertEquals("beijing", outRow1.getField(0));
        Assertions.assertEquals("zhangsan", outRow1.getField(1));

        // Test case 2: Null intermediate value (user.address is null, user.address.street should
        // return null)
        SeaTunnelRow userRow2 = new SeaTunnelRow(new Object[] {"lisi", null});
        SeaTunnelRow inputRow2 = new SeaTunnelRow(new Object[] {userRow2});

        SeaTunnelRow outRow2 = sqlEngine.transformBySQL(inputRow2, outRowType).get(0);

        // Verify that when intermediate value is null, the result should be null
        Assertions.assertNull(
                outRow2.getField(0),
                "When accessing nested field where intermediate value is null, result should be null");
        Assertions.assertEquals("lisi", outRow2.getField(1));
    }
}

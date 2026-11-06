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

package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.transform.sql.zeta.functions.JsonFunction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

public class SQLJsonFunctionsTest {

    private SeaTunnelRow runSql(String query, SeaTunnelRowType rowType, Object... values) {
        CatalogTable table = CatalogTableUtil.getCatalogTable("test", rowType);
        ReadonlyConfig config = ReadonlyConfig.fromMap(Collections.singletonMap("query", query));
        SQLTransform transform = new SQLTransform(config, table);
        List<SeaTunnelRow> out = transform.transformRow(new SeaTunnelRow(values));
        Assertions.assertNotNull(out);
        Assertions.assertFalse(out.isEmpty());
        return out.get(0);
    }

    private SeaTunnelRowType stringRowType(String field) {
        return new SeaTunnelRowType(
                new String[] {field}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
    }

    @Test
    public void testExtractsStringValue() {
        SeaTunnelRow out =
                runSql(
                        "select GET_JSON_OBJECT(name, '$.a') as r from dual",
                        stringRowType("name"),
                        "{\"a\":\"hello\"}");
        Assertions.assertEquals("hello", out.getField(0));
    }

    @Test
    public void testExtractsNumberValue() {
        SeaTunnelRow out =
                runSql(
                        "select GET_JSON_OBJECT(name, '$.a') as r from dual",
                        stringRowType("name"),
                        "{\"a\":42}");
        Assertions.assertEquals("42", out.getField(0));
    }

    @Test
    public void testExtractsDecimalNumber() {
        SeaTunnelRow out =
                runSql(
                        "select GET_JSON_OBJECT(name, '$.a') as r from dual",
                        stringRowType("name"),
                        "{\"a\":1.5}");
        Assertions.assertEquals("1.5", out.getField(0));
    }

    @Test
    public void testExtractsBooleanValue() {
        SeaTunnelRow out =
                runSql(
                        "select GET_JSON_OBJECT(name, '$.a') as r from dual",
                        stringRowType("name"),
                        "{\"a\":true}");
        Assertions.assertEquals("true", out.getField(0));
    }

    @Test
    public void testExtractsObjectAsRawJson() {
        SeaTunnelRow out =
                runSql(
                        "select GET_JSON_OBJECT(name, '$.a') as r from dual",
                        stringRowType("name"),
                        "{\"a\":{\"b\":1}}");
        Assertions.assertEquals("{\"b\":1}", out.getField(0));
    }

    @Test
    public void testExtractsArrayAsRawJson() {
        SeaTunnelRow out =
                runSql(
                        "select GET_JSON_OBJECT(name, '$.a') as r from dual",
                        stringRowType("name"),
                        "{\"a\":[1,2,3]}");
        Assertions.assertEquals("[1,2,3]", out.getField(0));
    }

    @Test
    public void testExtractsArrayElement() {
        SeaTunnelRow out =
                runSql(
                        "select GET_JSON_OBJECT(name, '$.a[1]') as r from dual",
                        stringRowType("name"),
                        "{\"a\":[10,20,30]}");
        Assertions.assertEquals("20", out.getField(0));
    }

    @Test
    public void testNestedPath() {
        SeaTunnelRow out =
                runSql(
                        "select GET_JSON_OBJECT(name, '$.a.b.c') as r from dual",
                        stringRowType("name"),
                        "{\"a\":{\"b\":{\"c\":\"deep\"}}}");
        Assertions.assertEquals("deep", out.getField(0));
    }

    @Test
    public void testNestedArrayAndObject() {
        SeaTunnelRow out =
                runSql(
                        "select GET_JSON_OBJECT(name, '$.a[0].b') as r from dual",
                        stringRowType("name"),
                        "{\"a\":[{\"b\":\"first\"},{\"b\":\"second\"}]}");
        Assertions.assertEquals("first", out.getField(0));
    }

    @Test
    public void testRootPathReturnsWholeDocument() {
        SeaTunnelRow out =
                runSql(
                        "select GET_JSON_OBJECT(name, '$') as r from dual",
                        stringRowType("name"),
                        "{\"a\":1}");
        Assertions.assertEquals("{\"a\":1}", out.getField(0));
    }

    @Test
    public void testNullJsonReturnsNull() {
        SeaTunnelRow out =
                runSql(
                        "select GET_JSON_OBJECT(name, '$.a') as r from dual",
                        stringRowType("name"),
                        (Object) null);
        Assertions.assertNull(out.getField(0));
    }

    @Test
    public void testInvalidJsonReturnsNull() {
        SeaTunnelRow out =
                runSql(
                        "select GET_JSON_OBJECT(name, '$.a') as r from dual",
                        stringRowType("name"),
                        "not a json");
        Assertions.assertNull(out.getField(0));
    }

    @Test
    public void testMissingPathReturnsNull() {
        SeaTunnelRow out =
                runSql(
                        "select GET_JSON_OBJECT(name, '$.missing') as r from dual",
                        stringRowType("name"),
                        "{\"a\":1}");
        Assertions.assertNull(out.getField(0));
    }

    @Test
    public void testJsonNullValueReturnsNull() {
        SeaTunnelRow out =
                runSql(
                        "select GET_JSON_OBJECT(name, '$.a') as r from dual",
                        stringRowType("name"),
                        "{\"a\":null}");
        Assertions.assertNull(out.getField(0));
    }

    @Test
    public void testArrayIndexOutOfBoundsReturnsNull() {
        SeaTunnelRow out =
                runSql(
                        "select GET_JSON_OBJECT(name, '$.a[5]') as r from dual",
                        stringRowType("name"),
                        "{\"a\":[1,2]}");
        Assertions.assertNull(out.getField(0));
    }

    @Test
    public void testFunctionNameIsCaseInsensitive() {
        // Function names are case-insensitive; the dispatcher upper-cases them.
        SeaTunnelRow out =
                runSql(
                        "select get_json_object(name, '$.a') as r from dual",
                        stringRowType("name"),
                        "{\"a\":\"ok\"}");
        Assertions.assertEquals("ok", out.getField(0));
    }

    @Test
    public void testUsedInCaseWhen() {
        // Reproduces the migration use case from the feature request.
        SeaTunnelRow out =
                runSql(
                        "select case when GET_JSON_OBJECT(name, '$.key1') = 'value1' then 'A'"
                                + " else 'B' end as r from dual",
                        stringRowType("name"),
                        "{\"key1\":\"value1\"}");
        Assertions.assertEquals("A", out.getField(0));
    }

    @Test
    public void testMalformedPathReturnsNull() {
        // Recursive descent, a trailing dot, and a path without the leading '$' are all rejected.
        SeaTunnelRowType rowType = stringRowType("name");
        Assertions.assertNull(
                runSql("select GET_JSON_OBJECT(name, '$..a') as r from dual", rowType, "{\"a\":1}")
                        .getField(0));
        Assertions.assertNull(
                runSql("select GET_JSON_OBJECT(name, '$.a.') as r from dual", rowType, "{\"a\":1}")
                        .getField(0));
        Assertions.assertNull(
                runSql(
                                "select GET_JSON_OBJECT(name, 'a.b') as r from dual",
                                rowType,
                                "{\"a\":{\"b\":1}}")
                        .getField(0));
    }

    @Test
    public void testArgumentCountGuard() {
        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () -> JsonFunction.getJsonObject(Collections.<Object>singletonList("only-one")));
    }
}

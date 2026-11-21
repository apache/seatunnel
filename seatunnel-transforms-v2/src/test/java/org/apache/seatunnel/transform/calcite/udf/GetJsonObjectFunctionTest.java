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

package org.apache.seatunnel.transform.calcite.udf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class GetJsonObjectFunctionTest {

    @Test
    void testExtractsStringValue() {
        Assertions.assertEquals("hello", GetJsonObjectFunction.eval("{\"a\":\"hello\"}", "$.a"));
    }

    @Test
    void testStringValueIsUnquoted() {
        // Returns the decoded string content, without surrounding quotes.
        Assertions.assertEquals(
                "a \"b\" c", GetJsonObjectFunction.eval("{\"k\":\"a \\\"b\\\" c\"}", "$.k"));
    }

    @Test
    void testExtractsNumberValue() {
        Assertions.assertEquals("42", GetJsonObjectFunction.eval("{\"a\":42}", "$.a"));
    }

    @Test
    void testExtractsDecimalNumber() {
        Assertions.assertEquals("1.5", GetJsonObjectFunction.eval("{\"a\":1.5}", "$.a"));
    }

    @Test
    void testExtractsBooleanValue() {
        Assertions.assertEquals("true", GetJsonObjectFunction.eval("{\"a\":true}", "$.a"));
        Assertions.assertEquals("false", GetJsonObjectFunction.eval("{\"a\":false}", "$.a"));
    }

    @Test
    void testExtractsObjectAsRawJson() {
        Assertions.assertEquals(
                "{\"b\":1}", GetJsonObjectFunction.eval("{\"a\":{\"b\":1}}", "$.a"));
    }

    @Test
    void testExtractsArrayAsRawJson() {
        Assertions.assertEquals("[1,2,3]", GetJsonObjectFunction.eval("{\"a\":[1,2,3]}", "$.a"));
    }

    @Test
    void testExtractsArrayElement() {
        Assertions.assertEquals("20", GetJsonObjectFunction.eval("{\"a\":[10,20,30]}", "$.a[1]"));
    }

    @Test
    void testNestedPath() {
        Assertions.assertEquals(
                "deep", GetJsonObjectFunction.eval("{\"a\":{\"b\":{\"c\":\"deep\"}}}", "$.a.b.c"));
    }

    @Test
    void testRootPathReturnsWholeDocument() {
        Assertions.assertEquals("{\"a\":1}", GetJsonObjectFunction.eval("{\"a\":1}", "$"));
    }

    @Test
    void testNullJsonReturnsNull() {
        Assertions.assertNull(GetJsonObjectFunction.eval(null, "$.a"));
    }

    @Test
    void testNullPathReturnsNull() {
        Assertions.assertNull(GetJsonObjectFunction.eval("{\"a\":1}", null));
    }

    @Test
    void testInvalidJsonReturnsNull() {
        Assertions.assertNull(GetJsonObjectFunction.eval("not a json", "$.a"));
    }

    @Test
    void testMissingPathReturnsNull() {
        Assertions.assertNull(GetJsonObjectFunction.eval("{\"a\":1}", "$.missing"));
    }

    @Test
    void testJsonNullValueReturnsNull() {
        Assertions.assertNull(GetJsonObjectFunction.eval("{\"a\":null}", "$.a"));
    }

    @Test
    void testArrayIndexOutOfBoundsReturnsNull() {
        Assertions.assertNull(GetJsonObjectFunction.eval("{\"a\":[1,2]}", "$.a[5]"));
    }

    @Test
    void testBracketQuotedKeyWithDot() {
        // Bracket-quoted field access lets keys contain '.' or '['.
        Assertions.assertEquals("v", GetJsonObjectFunction.eval("{\"a.b\":\"v\"}", "$['a.b']"));
    }

    @Test
    void testBracketQuotedKeyWithClosingBracket() {
        // The bracket scanner is quote-aware, so ']' inside a quoted key does not close early.
        Assertions.assertEquals("v", GetJsonObjectFunction.eval("{\"a]b\":\"v\"}", "$['a]b']"));
    }

    @Test
    void testFieldAccessOnNonObjectReturnsNull() {
        // .field on an array or scalar does not resolve.
        Assertions.assertNull(GetJsonObjectFunction.eval("{\"a\":[1,2]}", "$.a.b"));
        Assertions.assertNull(GetJsonObjectFunction.eval("{\"a\":1}", "$.a.b"));
    }

    @Test
    void testArrayIndexOnNonArrayReturnsNull() {
        // [n] on an object or scalar does not resolve.
        Assertions.assertNull(GetJsonObjectFunction.eval("{\"a\":{\"b\":1}}", "$.a[0]"));
        Assertions.assertNull(GetJsonObjectFunction.eval("{\"a\":\"str\"}", "$.a[0]"));
    }

    @Test
    void testNegativeArrayIndexReturnsNull() {
        Assertions.assertNull(GetJsonObjectFunction.eval("[1,2]", "$[-1]"));
    }

    @Test
    void testConsecutiveArrayIndexes() {
        Assertions.assertEquals("20", GetJsonObjectFunction.eval("[[10,20],[30,40]]", "$[0][1]"));
    }

    @Test
    void testRootArrayAccess() {
        Assertions.assertEquals("10", GetJsonObjectFunction.eval("[10,20]", "$[0]"));
    }

    @Test
    void testScalarRootReturnsScalar() {
        // A scalar root document is rendered as text, not wrapped.
        Assertions.assertEquals("42", GetJsonObjectFunction.eval("42", "$"));
        Assertions.assertEquals("hello", GetJsonObjectFunction.eval("\"hello\"", "$"));
    }

    @Test
    void testFunctionName() {
        GetJsonObjectFunction fn = new GetJsonObjectFunction();
        Assertions.assertEquals("GET_JSON_OBJECT", fn.functionName());
    }
}

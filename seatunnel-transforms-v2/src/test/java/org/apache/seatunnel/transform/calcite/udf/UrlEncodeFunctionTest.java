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

class UrlEncodeFunctionTest {

    @Test
    void testNullInput() {
        Assertions.assertNull(UrlEncodeFunction.eval(null));
    }

    @Test
    void testPlainText() {
        Assertions.assertEquals("seatunnel", UrlEncodeFunction.eval("seatunnel"));
    }

    @Test
    void testSpacesEncoded() {
        Assertions.assertEquals("sea+tunnel", UrlEncodeFunction.eval("sea tunnel"));
    }

    @Test
    void testSpecialCharacters() {
        String result = UrlEncodeFunction.eval("source=jdbc&sink=kafka");
        Assertions.assertEquals("source%3Djdbc%26sink%3Dkafka", result);
    }

    @Test
    void testUrlPathCharacters() {
        String result = UrlEncodeFunction.eval("seatunnel/connector?type=source");
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.contains("%2F"));
        Assertions.assertTrue(result.contains("%3F"));
    }

    @Test
    void testEmptyString() {
        Assertions.assertEquals("", UrlEncodeFunction.eval(""));
    }

    @Test
    void testFunctionName() {
        UrlEncodeFunction fn = new UrlEncodeFunction();
        Assertions.assertEquals("URL_ENCODE", fn.functionName());
    }
}

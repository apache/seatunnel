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

class UrlDecodeFunctionTest {

    @Test
    void testNullInput() {
        Assertions.assertNull(UrlDecodeFunction.eval(null));
    }

    @Test
    void testPlainText() {
        Assertions.assertEquals("seatunnel", UrlDecodeFunction.eval("seatunnel"));
    }

    @Test
    void testDecodeSpaces() {
        Assertions.assertEquals("sea tunnel", UrlDecodeFunction.eval("sea+tunnel"));
    }

    @Test
    void testDecodeSpecialCharacters() {
        Assertions.assertEquals(
                "source=jdbc&sink=kafka", UrlDecodeFunction.eval("source%3Djdbc%26sink%3Dkafka"));
    }

    @Test
    void testRoundTripWithEncode() {
        String original = "seatunnel transform! @#$%^&*()";
        String encoded = UrlEncodeFunction.eval(original);
        String decoded = UrlDecodeFunction.eval(encoded);
        Assertions.assertEquals(original, decoded);
    }

    @Test
    void testRoundTripWithPath() {
        String original = "seatunnel/connector?type=source&format=json";
        String encoded = UrlEncodeFunction.eval(original);
        String decoded = UrlDecodeFunction.eval(encoded);
        Assertions.assertEquals(original, decoded);
    }

    @Test
    void testEmptyString() {
        Assertions.assertEquals("", UrlDecodeFunction.eval(""));
    }

    @Test
    void testFunctionName() {
        UrlDecodeFunction fn = new UrlDecodeFunction();
        Assertions.assertEquals("URL_DECODE", fn.functionName());
    }
}

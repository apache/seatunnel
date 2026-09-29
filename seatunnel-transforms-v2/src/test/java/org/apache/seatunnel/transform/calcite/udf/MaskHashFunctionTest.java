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

class MaskHashFunctionTest {

    @Test
    void testNullInput() {
        Assertions.assertNull(MaskHashFunction.eval(null));
    }

    @Test
    void testNonNullReturns64CharHex() {
        String result = MaskHashFunction.eval("seatunnel-transform");
        Assertions.assertNotNull(result);
        Assertions.assertEquals(64, result.length());
        Assertions.assertTrue(result.matches("[0-9a-f]{64}"));
    }

    @Test
    void testDeterministic() {
        String first = MaskHashFunction.eval("seatunnel");
        String second = MaskHashFunction.eval("seatunnel");
        Assertions.assertEquals(first, second);
    }

    @Test
    void testDifferentInputsDifferentHash() {
        String hash1 = MaskHashFunction.eval("connector-source");
        String hash2 = MaskHashFunction.eval("connector-sink");
        Assertions.assertNotEquals(hash1, hash2);
    }

    @Test
    void testEmptyString() {
        String result = MaskHashFunction.eval("");
        Assertions.assertNotNull(result);
        Assertions.assertEquals(64, result.length());
    }

    @Test
    void testKnownSHA256() {
        // SHA-256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
        Assertions.assertEquals(
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                MaskHashFunction.eval("abc"));
    }

    @Test
    void testFunctionName() {
        MaskHashFunction fn = new MaskHashFunction();
        Assertions.assertEquals("MASK_HASH", fn.functionName());
    }
}

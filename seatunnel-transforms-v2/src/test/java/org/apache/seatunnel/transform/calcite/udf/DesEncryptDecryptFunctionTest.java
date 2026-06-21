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

class DesEncryptDecryptFunctionTest {

    private static final String PASSWORD = "12345678";

    @Test
    void testEncryptNullPassword() {
        Assertions.assertNull(DesEncryptFunction.eval(null, "seatunnel"));
    }

    @Test
    void testEncryptNullData() {
        Assertions.assertNull(DesEncryptFunction.eval(PASSWORD, null));
    }

    @Test
    void testDecryptNullPassword() {
        Assertions.assertNull(DesDecryptFunction.eval(null, "seatunnel"));
    }

    @Test
    void testDecryptNullData() {
        Assertions.assertNull(DesDecryptFunction.eval(PASSWORD, null));
    }

    @Test
    void testEncryptProducesNonNullResult() {
        String result = DesEncryptFunction.eval(PASSWORD, "seatunnel-transform");
        Assertions.assertNotNull(result);
        Assertions.assertFalse(result.isEmpty());
    }

    @Test
    void testEncryptDecryptRoundTrip() {
        String original = "seatunnel-connector-v2";
        String encrypted = DesEncryptFunction.eval(PASSWORD, original);
        String decrypted = DesDecryptFunction.eval(PASSWORD, encrypted);
        Assertions.assertEquals(original, decrypted);
    }

    @Test
    void testRoundTripLongText() {
        String original = "apache-seatunnel-zeta-engine-checkpoint";
        String encrypted = DesEncryptFunction.eval(PASSWORD, original);
        String decrypted = DesDecryptFunction.eval(PASSWORD, encrypted);
        Assertions.assertEquals(original, decrypted);
    }

    @Test
    void testDifferentPasswordsProduceDifferentCiphertext() {
        String data = "seatunnel";
        String enc1 = DesEncryptFunction.eval("abcdefgh", data);
        String enc2 = DesEncryptFunction.eval("12345678", data);
        Assertions.assertNotEquals(enc1, enc2);
    }

    @Test
    void testEncryptDeterministic() {
        String first = DesEncryptFunction.eval(PASSWORD, "seatunnel");
        String second = DesEncryptFunction.eval(PASSWORD, "seatunnel");
        Assertions.assertEquals(first, second);
    }

    @Test
    void testFunctionNames() {
        Assertions.assertEquals("DES_ENCRYPT", new DesEncryptFunction().functionName());
        Assertions.assertEquals("DES_DECRYPT", new DesDecryptFunction().functionName());
    }
}

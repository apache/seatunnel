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

package org.apache.seatunnel.transform.sql.zeta.functions.udf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DESUtilTest {

    @Test
    public void testEncryptDecryptRoundTrip() {
        String password = "password123";
        String data = "hello-world";

        String encrypted = DESUtil.encrypt(password, data);
        Assertions.assertNotNull(encrypted);
        Assertions.assertNotEquals(data, encrypted);

        String decrypted = DESUtil.decrypt(password, encrypted);
        Assertions.assertEquals(data, decrypted);
    }

    @Test
    public void testEncryptAndDecryptNullData() {
        String password = "password123";
        Assertions.assertNull(DESUtil.encrypt(password, null));
        Assertions.assertNull(DESUtil.decrypt(password, null));
    }

    @Test
    public void testEncryptShortPasswordThrows() {
        Assertions.assertThrows(RuntimeException.class, () -> DESUtil.encrypt("short", "data"));
    }

    @Test
    public void testDecryptShortPasswordThrows() {
        Assertions.assertThrows(RuntimeException.class, () -> DESUtil.decrypt("short", "cipher"));
    }
}

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

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import javax.crypto.spec.IvParameterSpec;

import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;

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

    @Test
    public void testRepeatedPlaintextUsesDifferentIv() {
        Set<String> ivs = new HashSet<>();
        Set<String> ciphertexts = new HashSet<>();
        for (int i = 0; i < 128; i++) {
            String encrypted = DESUtil.encrypt("password123", "same-input");
            Assertions.assertTrue(encrypted.startsWith("STDES1:"));
            byte[] payload = Base64.getDecoder().decode(encrypted.substring(7));
            Assertions.assertTrue(
                    ivs.add(Base64.getEncoder().encodeToString(Arrays.copyOf(payload, 8))));
            Assertions.assertTrue(ciphertexts.add(encrypted));
            Assertions.assertEquals("same-input", DESUtil.decrypt("password123", encrypted));
        }
    }

    @Test
    public void testLegacyCiphertextStillDecrypts() throws Exception {
        String password = "password123";
        String plaintext = "existing-data";
        Cipher cipher = Cipher.getInstance("DES/CBC/PKCS5Padding");
        Key key =
                SecretKeyFactory.getInstance("DES")
                        .generateSecret(new DESKeySpec(password.getBytes(StandardCharsets.UTF_8)));
        cipher.init(
                Cipher.ENCRYPT_MODE,
                key,
                new IvParameterSpec("12345678".getBytes(StandardCharsets.UTF_8)));
        String legacy =
                Base64.getEncoder()
                        .encodeToString(cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8)));
        Assertions.assertEquals(plaintext, DESUtil.decrypt(password, legacy));
        Assertions.assertNotEquals(legacy, DESUtil.encrypt(password, plaintext));
    }

    @Test
    public void testMalformedNewFormatIsNotTreatedAsLegacy() {
        for (String value :
                new String[] {
                    "STDES1:", "STDES1:AAAA", "STDES1:!invalid!", "STDES1:AAAAAAAAAAAAAAAAAAAAAA=="
                }) {
            Assertions.assertThrows(
                    IllegalArgumentException.class, () -> DESUtil.decrypt("password123", value));
        }
    }

    @Test
    public void testEmptyUnicodeAndBlockBoundaryRoundTrip() {
        for (String value : new String[] {"", "中文😀", "1234567", "12345678", "123456789"}) {
            Assertions.assertEquals(
                    value, DESUtil.decrypt("password123", DESUtil.encrypt("password123", value)));
        }
    }

    @Test
    public void testLegacyInvalidDataBehaviorIsPreserved() {
        Assertions.assertEquals("not-base64", DESUtil.decrypt("password123", "not-base64"));
    }
}

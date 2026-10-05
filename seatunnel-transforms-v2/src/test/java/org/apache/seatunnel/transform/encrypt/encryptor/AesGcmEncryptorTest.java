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

package org.apache.seatunnel.transform.encrypt.encryptor;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AesGcmEncryptorTest {

    private AesGcmEncryptor encryptor;

    private static final String TEST_KEY =
            "base64:"
                    + Base64.getEncoder()
                            .encodeToString(
                                    "1234567890123456"
                                            .getBytes(java.nio.charset.StandardCharsets.UTF_8));

    @BeforeEach
    void setUp() {
        encryptor = new AesGcmEncryptor();
        encryptor.init(TEST_KEY);
    }

    @Test
    void testEncryptAndDecrypt() {
        String plain = "test-text";

        String cipher = encryptor.encrypt(plain);
        String decrypted = encryptor.decrypt(cipher);

        assertEquals(plain, decrypted);
    }

    @Test
    void testEncryptProducesDifferentCipherText() {
        String plain = "same-text";

        String cipher1 = encryptor.encrypt(plain);
        String cipher2 = encryptor.encrypt(plain);

        // GCM uses random IV so ciphertext should differ
        assertNotEquals(cipher1, cipher2);
    }

    @Test
    void testDecryptTamperedCipherText() {
        String plain = "secure-text";

        String cipher = encryptor.encrypt(plain);

        byte[] decoded = Base64.getDecoder().decode(cipher);

        // tamper with ciphertext
        decoded[decoded.length - 1] ^= 1;

        String tampered = Base64.getEncoder().encodeToString(decoded);

        assertThrows(SeaTunnelRuntimeException.class, () -> encryptor.decrypt(tampered));
    }

    @Test
    void testInvalidCipherTextTooShort() {
        String invalid = Base64.getEncoder().encodeToString(new byte[5]);

        SeaTunnelRuntimeException ex =
                assertThrows(SeaTunnelRuntimeException.class, () -> encryptor.decrypt(invalid));

        assertTrue(ex.getMessage().contains("Invalid encrypted value (too short)"));
    }

    @Test
    void testDecryptWithWrongKey() {
        String plain = "hello";

        String cipher = encryptor.encrypt(plain);

        AesGcmEncryptor another = new AesGcmEncryptor();

        String otherKey =
                "base64:"
                        + Base64.getEncoder()
                                .encodeToString(
                                        "abcdefabcdefabcd"
                                                .getBytes(java.nio.charset.StandardCharsets.UTF_8));

        another.init(otherKey);

        SeaTunnelRuntimeException ex =
                assertThrows(SeaTunnelRuntimeException.class, () -> another.decrypt(cipher));
        assertTrue(ex.getMessage().contains("Decryption failed (possible tampering or wrong key)"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "  ", "\t", "\n"})
    void testEmptyOrWhitespaceString(String plain) {
        String cipher = encryptor.encrypt(plain);
        String decrypt = encryptor.decrypt(cipher);

        assertEquals(plain, decrypt);
    }

    @Test
    void testSupportAlgorithm() {
        assertTrue(encryptor.support(AesGcmEncryptor.IDENTIFIER));
        assertFalse(encryptor.support(AesCbcEncryptor.IDENTIFIER));
    }
}

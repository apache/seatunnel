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

package org.apache.seatunnel.transform.sql.zeta.functions;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class CryptoFunctionTest {

    private static final String PASSPHRASE = "mySecretPass";
    private static final String EXPLICIT_IV = "1234567890123456"; // 16 bytes
    private static final String PLAINTEXT = "Hello SeaTunnel";

    private List<Object> args(Object... values) {
        List<Object> list = new ArrayList<>();
        for (Object v : values) {
            list.add(v);
        }
        return list;
    }

    @Test
    public void testRoundTripWithPassphraseAndRandomIv() {
        String cipher = CryptoFunction.aesEncrypt(args(PLAINTEXT, PASSPHRASE));
        Assertions.assertNotNull(cipher);
        Assertions.assertNotEquals(PLAINTEXT, cipher);
        String decrypted = CryptoFunction.aesDecrypt(args(cipher, PASSPHRASE));
        Assertions.assertEquals(PLAINTEXT, decrypted);
    }

    @Test
    public void testRoundTripWithPassphraseAndExplicitIv() {
        String cipher = CryptoFunction.aesEncrypt(args(PLAINTEXT, PASSPHRASE, EXPLICIT_IV));
        Assertions.assertNotNull(cipher);
        // With an explicit IV the ciphertext does not carry the IV prefix.
        String decrypted = CryptoFunction.aesDecrypt(args(cipher, PASSPHRASE, EXPLICIT_IV));
        Assertions.assertEquals(PLAINTEXT, decrypted);
    }

    @Test
    public void testRoundTripWithBase64KeyAndRandomIv() {
        byte[] rawKey = new byte[16];
        for (int i = 0; i < rawKey.length; i++) {
            rawKey[i] = (byte) i;
        }
        String base64Key = "base64:" + Base64.getEncoder().encodeToString(rawKey);

        String cipher = CryptoFunction.aesEncrypt(args(PLAINTEXT, base64Key));
        Assertions.assertNotNull(cipher);
        String decrypted = CryptoFunction.aesDecrypt(args(cipher, base64Key));
        Assertions.assertEquals(PLAINTEXT, decrypted);
    }

    @Test
    public void testRoundTripWithBase64KeyAndExplicitIv() {
        byte[] rawKey = new byte[32];
        for (int i = 0; i < rawKey.length; i++) {
            rawKey[i] = (byte) (i + 1);
        }
        String base64Key = "base64:" + Base64.getEncoder().encodeToString(rawKey);

        String cipher = CryptoFunction.aesEncrypt(args(PLAINTEXT, base64Key, EXPLICIT_IV));
        String decrypted = CryptoFunction.aesDecrypt(args(cipher, base64Key, EXPLICIT_IV));
        Assertions.assertEquals(PLAINTEXT, decrypted);
    }

    @Test
    public void testRandomIvProducesDifferentCiphertext() {
        String cipher1 = CryptoFunction.aesEncrypt(args(PLAINTEXT, PASSPHRASE));
        String cipher2 = CryptoFunction.aesEncrypt(args(PLAINTEXT, PASSPHRASE));
        // A random IV makes the ciphertext non-deterministic.
        Assertions.assertNotEquals(cipher1, cipher2);
        Assertions.assertEquals(PLAINTEXT, CryptoFunction.aesDecrypt(args(cipher1, PASSPHRASE)));
        Assertions.assertEquals(PLAINTEXT, CryptoFunction.aesDecrypt(args(cipher2, PASSPHRASE)));
    }

    @Test
    public void testExplicitIvIsDeterministic() {
        String cipher1 = CryptoFunction.aesEncrypt(args(PLAINTEXT, PASSPHRASE, EXPLICIT_IV));
        String cipher2 = CryptoFunction.aesEncrypt(args(PLAINTEXT, PASSPHRASE, EXPLICIT_IV));
        Assertions.assertEquals(cipher1, cipher2);
    }

    @Test
    public void testEncryptNullValueReturnsNull() {
        Assertions.assertNull(CryptoFunction.aesEncrypt(args(null, PASSPHRASE)));
    }

    @Test
    public void testDecryptNullValueReturnsNull() {
        Assertions.assertNull(CryptoFunction.aesDecrypt(args(null, PASSPHRASE)));
    }

    @Test
    public void testEncryptTooFewArgsThrows() {
        Assertions.assertThrows(
                SeaTunnelRuntimeException.class, () -> CryptoFunction.aesEncrypt(args(PLAINTEXT)));
    }

    @Test
    public void testEncryptTooManyArgsThrows() {
        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () -> CryptoFunction.aesEncrypt(args(PLAINTEXT, PASSPHRASE, EXPLICIT_IV, "extra")));
    }

    @Test
    public void testDecryptTooFewArgsThrows() {
        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () -> CryptoFunction.aesDecrypt(args("ciphertext")));
    }

    @Test
    public void testDecryptTooManyArgsThrows() {
        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () ->
                        CryptoFunction.aesDecrypt(
                                args("ciphertext", PASSPHRASE, EXPLICIT_IV, "extra")));
    }

    @Test
    public void testEncryptWithNullKeyThrows() {
        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () -> CryptoFunction.aesEncrypt(args(PLAINTEXT, null)));
    }

    @Test
    public void testDecryptWithNullKeyThrows() {
        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () -> CryptoFunction.aesDecrypt(args("ciphertext", null)));
    }

    @Test
    public void testEncryptWithEmptyKeyThrows() {
        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () -> CryptoFunction.aesEncrypt(args(PLAINTEXT, "")));
    }

    @Test
    public void testEncryptWithBlankKeyThrows() {
        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () -> CryptoFunction.aesEncrypt(args(PLAINTEXT, "   ")));
    }

    @Test
    public void testBase64KeyMalformedThrows() {
        // The base64: prefix is present but the remainder is not valid Base64 content.
        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () -> CryptoFunction.aesEncrypt(args(PLAINTEXT, "base64:@@@@")));
    }

    @Test
    public void testDecryptWithWrongKeyDoesNotLeakPlaintext() {
        String cipher = CryptoFunction.aesEncrypt(args(PLAINTEXT, PASSPHRASE));
        // CBC/PKCS5Padding decryption with a wrong key usually fails the padding check, but a
        // random block can pass it (~1/256). Both outcomes are acceptable: throwing, or returning
        // garbage, as long as the real plaintext is never recovered.
        try {
            String result = CryptoFunction.aesDecrypt(args(cipher, "wrongPassphrase"));
            Assertions.assertNotEquals(PLAINTEXT, result);
        } catch (SeaTunnelRuntimeException e) {
            // expected: padding/auth check rejected the wrong key
        }
    }

    @Test
    public void testExplicitIvWrongLengthThrows() {
        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () -> CryptoFunction.aesEncrypt(args(PLAINTEXT, PASSPHRASE, "short")));
    }

    @Test
    public void testBase64KeyWrongLengthThrows() {
        byte[] rawKey = new byte[10]; // invalid AES key length
        String base64Key = "base64:" + Base64.getEncoder().encodeToString(rawKey);
        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () -> CryptoFunction.aesEncrypt(args(PLAINTEXT, base64Key)));
    }

    @Test
    public void testDecryptInvalidBase64Throws() {
        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () -> CryptoFunction.aesDecrypt(args("@@invalid@@", PASSPHRASE)));
    }

    @Test
    public void testDecryptTooShortPayloadThrows() {
        // A valid Base64 string that decodes to fewer than 16 bytes cannot carry the IV.
        String shortBase64 = Base64.getEncoder().encodeToString(new byte[8]);
        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () -> CryptoFunction.aesDecrypt(args(shortBase64, PASSPHRASE)));
    }

    @Test
    public void testRoundTripUnicodePlaintext() {
        String unicode = "加密测试🔐数据";
        String cipher = CryptoFunction.aesEncrypt(args(unicode, PASSPHRASE));
        Assertions.assertEquals(unicode, CryptoFunction.aesDecrypt(args(cipher, PASSPHRASE)));
    }

    @Test
    public void testExplicitIvCiphertextCannotBeDecryptedWithoutIv() {
        String cipher = CryptoFunction.aesEncrypt(args(PLAINTEXT, PASSPHRASE, EXPLICIT_IV));
        // The explicit-IV ciphertext carries no IV prefix, so decrypting without an IV interprets
        // the first 16 ciphertext bytes as the IV and decrypts the rest with it. This must not
        // return the original plaintext (it throws on a padding failure, or yields garbage).
        try {
            String result = CryptoFunction.aesDecrypt(args(cipher, PASSPHRASE));
            Assertions.assertNotEquals(PLAINTEXT, result);
        } catch (SeaTunnelRuntimeException e) {
            // expected
        }
    }

    @Test
    public void testRandomIvCiphertextRoundTripsWithoutExplicitIv() {
        // Ciphertext produced without an IV embeds the IV in its first 16 bytes; decrypting
        // without an IV must recover the original plaintext.
        String cipher = CryptoFunction.aesEncrypt(args(PLAINTEXT, PASSPHRASE));
        Assertions.assertEquals(PLAINTEXT, CryptoFunction.aesDecrypt(args(cipher, PASSPHRASE)));
    }

    @Test
    public void testPassphraseKeyDerivationIsDeterministic() {
        // Two encryptions with the same passphrase and explicit IV must produce the same
        // ciphertext, proving the SHA-256 key derivation is deterministic.
        String cipher1 = CryptoFunction.aesEncrypt(args(PLAINTEXT, PASSPHRASE, EXPLICIT_IV));
        String cipher2 = CryptoFunction.aesEncrypt(args(PLAINTEXT, PASSPHRASE, EXPLICIT_IV));
        Assertions.assertEquals(cipher1, cipher2);
        Assertions.assertNotNull(
                Base64.getDecoder().decode(cipher1.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testRoundTripWithAes192Base64Key() {
        // Covers the 24-byte (AES-192) branch of the raw key length validation.
        byte[] rawKey = new byte[24];
        for (int i = 0; i < rawKey.length; i++) {
            rawKey[i] = (byte) (i + 5);
        }
        String base64Key = "base64:" + Base64.getEncoder().encodeToString(rawKey);
        String cipher = CryptoFunction.aesEncrypt(args(PLAINTEXT, base64Key, EXPLICIT_IV));
        Assertions.assertEquals(
                PLAINTEXT, CryptoFunction.aesDecrypt(args(cipher, base64Key, EXPLICIT_IV)));
    }

    @Test
    public void testDecryptExplicitIvEmptyPayloadThrows() {
        // An empty ciphertext combined with an explicit IV has nothing to decrypt.
        Assertions.assertThrows(
                SeaTunnelRuntimeException.class,
                () -> CryptoFunction.aesDecrypt(args("", PASSPHRASE, EXPLICIT_IV)));
    }

    @Test
    public void testNullIvArgFallsBackToRandomIv() {
        // A null third argument is treated as "no IV provided": a random IV is generated and
        // embedded in the ciphertext, so decrypting with a null IV argument must still round-trip.
        String cipher = CryptoFunction.aesEncrypt(args(PLAINTEXT, PASSPHRASE, null));
        Assertions.assertEquals(
                PLAINTEXT, CryptoFunction.aesDecrypt(args(cipher, PASSPHRASE, null)));
    }
}

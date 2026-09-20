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

import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.transform.exception.TransformCommonError;
import org.apache.seatunnel.transform.sql.zeta.ZetaSQLFunction;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;

public class CryptoFunction {

    private static final String TRANSFORMATION = "AES/CBC/PKCS5Padding";
    private static final String KEY_ALGORITHM = "AES";
    private static final int IV_SIZE = 16;
    private static final String BASE64_PREFIX = "base64:";

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private CryptoFunction() {}

    /**
     * Encrypts a value with AES/CBC/PKCS5Padding and returns a Base64 string.
     *
     * <p>When no IV is provided, a random 16-byte IV is generated and prepended to the ciphertext
     * so {@link #aesDecrypt(List)} can recover it without an explicit IV. When an IV is provided,
     * the caller manages it and the returned ciphertext contains only the encrypted bytes.
     *
     * @param args [value, key[, iv]]
     * @return Base64-encoded ciphertext, or null if the value is null
     */
    public static String aesEncrypt(List<Object> args) {
        if (args.size() < 2 || args.size() > 3) {
            throw CommonError.illegalArgument(String.valueOf(args), ZetaSQLFunction.AES_ENCRYPT);
        }
        Object value = args.get(0);
        if (value == null) {
            return null;
        }
        String plainText = value.toString();
        Object keyArg = args.get(1);
        if (keyArg == null) {
            throw CommonError.illegalArgument("null", ZetaSQLFunction.AES_ENCRYPT);
        }
        SecretKeySpec keySpec = buildKey(keyArg.toString(), ZetaSQLFunction.AES_ENCRYPT);

        boolean ivProvided = args.size() == 3 && args.get(2) != null;
        byte[] iv;
        if (ivProvided) {
            iv = buildIv(args.get(2).toString(), ZetaSQLFunction.AES_ENCRYPT);
        } else {
            iv = new byte[IV_SIZE];
            SECURE_RANDOM.nextBytes(iv);
        }

        try {
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(iv));
            byte[] encrypted = cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8));
            if (ivProvided) {
                return Base64.getEncoder().encodeToString(encrypted);
            }
            byte[] encryptedWithIv = new byte[IV_SIZE + encrypted.length];
            System.arraycopy(iv, 0, encryptedWithIv, 0, IV_SIZE);
            System.arraycopy(encrypted, 0, encryptedWithIv, IV_SIZE, encrypted.length);
            return Base64.getEncoder().encodeToString(encryptedWithIv);
        } catch (Exception e) {
            throw TransformCommonError.encryptionError(ZetaSQLFunction.AES_ENCRYPT, e);
        }
    }

    /**
     * Decrypts a Base64 AES/CBC/PKCS5Padding ciphertext.
     *
     * <p>When no IV is provided, the first 16 bytes of the decoded payload are treated as the IV
     * (the format produced by {@link #aesEncrypt(List)} without an IV). When an IV is provided, the
     * whole decoded payload is treated as the ciphertext and the caller-supplied IV is used.
     *
     * @param args [ciphertext, key[, iv]]
     * @return decrypted UTF-8 plaintext, or null if the value is null
     */
    public static String aesDecrypt(List<Object> args) {
        if (args.size() < 2 || args.size() > 3) {
            throw CommonError.illegalArgument(String.valueOf(args), ZetaSQLFunction.AES_DECRYPT);
        }
        Object value = args.get(0);
        if (value == null) {
            return null;
        }
        String cipherText = value.toString();
        Object keyArg = args.get(1);
        if (keyArg == null) {
            throw CommonError.illegalArgument("null", ZetaSQLFunction.AES_DECRYPT);
        }
        SecretKeySpec keySpec = buildKey(keyArg.toString(), ZetaSQLFunction.AES_DECRYPT);

        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(cipherText);
        } catch (IllegalArgumentException e) {
            throw CommonError.illegalArgument(cipherText, ZetaSQLFunction.AES_DECRYPT);
        }

        byte[] iv;
        byte[] encrypted;
        boolean ivProvided = args.size() == 3 && args.get(2) != null;
        if (ivProvided) {
            iv = buildIv(args.get(2).toString(), ZetaSQLFunction.AES_DECRYPT);
            encrypted = decoded;
            if (encrypted.length == 0) {
                throw CommonError.illegalArgument(cipherText, ZetaSQLFunction.AES_DECRYPT);
            }
        } else {
            if (decoded.length < IV_SIZE) {
                throw CommonError.illegalArgument(cipherText, ZetaSQLFunction.AES_DECRYPT);
            }
            iv = new byte[IV_SIZE];
            encrypted = new byte[decoded.length - IV_SIZE];
            System.arraycopy(decoded, 0, iv, 0, IV_SIZE);
            System.arraycopy(decoded, IV_SIZE, encrypted, 0, encrypted.length);
        }

        try {
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, keySpec, new IvParameterSpec(iv));
            byte[] original = cipher.doFinal(encrypted);
            return new String(original, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw TransformCommonError.encryptionError(ZetaSQLFunction.AES_DECRYPT, e);
        }
    }

    /**
     * Builds an AES key from either a raw Base64 key or a passphrase.
     *
     * <p>If the key starts with {@code base64:}, the remainder is decoded as a raw AES key and must
     * be 16, 24, or 32 bytes (AES-128/192/256). Otherwise the passphrase is hashed with SHA-256 and
     * the first 16 bytes are used as an AES-128 key, so arbitrary-length passphrases are supported.
     */
    private static SecretKeySpec buildKey(String key, String operation) {
        if (key == null || key.trim().isEmpty()) {
            throw CommonError.illegalArgument(String.valueOf(key), operation);
        }
        byte[] keyBytes;
        if (key.startsWith(BASE64_PREFIX)) {
            String base64 = key.substring(BASE64_PREFIX.length()).trim();
            try {
                keyBytes = Base64.getDecoder().decode(base64);
            } catch (IllegalArgumentException e) {
                throw CommonError.illegalArgument(key, operation);
            }
            if (!(keyBytes.length == 16 || keyBytes.length == 24 || keyBytes.length == 32)) {
                throw CommonError.illegalArgument(key, operation);
            }
        } else {
            keyBytes = derivePassphraseKey(key, operation);
        }
        return new SecretKeySpec(keyBytes, KEY_ALGORITHM);
    }

    private static byte[] derivePassphraseKey(String passphrase, String operation) {
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            byte[] digest = sha256.digest(passphrase.getBytes(StandardCharsets.UTF_8));
            byte[] keyBytes = new byte[16];
            System.arraycopy(digest, 0, keyBytes, 0, 16);
            return keyBytes;
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandated by every JDK; if it is missing the runtime is unusable.
            throw TransformCommonError.encryptionError(operation, e);
        }
    }

    private static byte[] buildIv(String iv, String operation) {
        byte[] ivBytes = iv.getBytes(StandardCharsets.UTF_8);
        if (ivBytes.length != IV_SIZE) {
            throw CommonError.illegalArgument(iv, operation);
        }
        return ivBytes;
    }
}

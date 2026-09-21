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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Built-in AES crypto functions for the Zeta SQL transform.
 *
 * <p>Both functions use {@code AES/CBC/PKCS5Padding} and return / accept Base64-encoded strings.
 * The wire format when no explicit IV is supplied is {@code Base64(IV || ciphertext)} (a random
 * 16-byte IV is generated per call and prepended), so {@link #aesDecrypt(List)} can recover it
 * without an explicit IV. When an explicit 16-byte IV is supplied, the ciphertext carries only the
 * encrypted bytes and the caller is responsible for the IV.
 *
 * <p>Key conventions:
 *
 * <ul>
 *   <li>a key prefixed with {@code base64:} is decoded as a raw AES key (16/24/32 bytes for
 *       AES-128/192/256); only this form is wire-compatible with the {@code AesCbcEncryptor} of the
 *       {@code FieldEncryptTransform};
 *   <li>any other key is treated as a passphrase and derived via a single unsalted SHA-256 whose
 *       first 16 bytes are used as an AES-128 key. This is fast to brute-force for low-entropy
 *       passphrases; use a random {@code base64:} key for strong protection.
 * </ul>
 *
 * <p>CBC is unauthenticated: a wrong key or corrupted ciphertext may (about once in 256) decrypt to
 * garbage instead of throwing.
 *
 * <p>Error paths never embed the key, plaintext, ciphertext or IV into the exception message; only
 * non-sensitive metadata (argument count, byte length, type name) is included, following the {@link
 * CommonError#illegalArgument(String, String)} convention used across the codebase.
 */
public class CryptoFunction {

    private static final String TRANSFORMATION = "AES/CBC/PKCS5Padding";
    private static final String KEY_ALGORITHM = "AES";
    private static final int IV_SIZE = 16;
    private static final String BASE64_PREFIX = "base64:";

    // Bounded cache for derived keys, keyed by the key string. A constant key (the common case)
    // is derived once instead of on every row.
    private static final int KEY_CACHE_MAX_SIZE = 64;
    private static final ConcurrentHashMap<String, SecretKeySpec> KEY_CACHE =
            new ConcurrentHashMap<>();

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private CryptoFunction() {}

    /**
     * Encrypts a value with AES/CBC/PKCS5Padding and returns a Base64 string.
     *
     * @param args [value, key[, iv]]
     * @return Base64-encoded ciphertext, or null if the value is null
     */
    public static String aesEncrypt(List<Object> args) {
        String operation = ZetaSQLFunction.AES_ENCRYPT;
        if (args.size() < 2 || args.size() > 3) {
            throw CommonError.illegalArgument(
                    String.valueOf(args.size()), operation + " expects 2 or 3 arguments");
        }
        Object value = args.get(0);
        if (value == null) {
            return null;
        }
        rejectNonScalar(value, operation);
        String plainText = value.toString();
        Object keyArg = args.get(1);
        if (keyArg == null) {
            throw CommonError.illegalArgument("key", operation + ": key must not be null");
        }
        SecretKeySpec keySpec = cachedKey(keyArg.toString(), operation);

        byte[] iv = resolveIv(args, operation, true);
        boolean ivProvided = args.size() == 3;

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
            throw TransformCommonError.encryptionError(operation + ": encryption failed", e);
        }
    }

    /**
     * Decrypts a Base64 AES/CBC/PKCS5Padding ciphertext.
     *
     * @param args [ciphertext, key[, iv]]
     * @return decrypted UTF-8 plaintext, or null if the value is null
     */
    public static String aesDecrypt(List<Object> args) {
        String operation = ZetaSQLFunction.AES_DECRYPT;
        if (args.size() < 2 || args.size() > 3) {
            throw CommonError.illegalArgument(
                    String.valueOf(args.size()), operation + " expects 2 or 3 arguments");
        }
        Object value = args.get(0);
        if (value == null) {
            return null;
        }
        rejectNonScalar(value, operation);
        String cipherText = value.toString();
        Object keyArg = args.get(1);
        if (keyArg == null) {
            throw CommonError.illegalArgument("key", operation + ": key must not be null");
        }
        SecretKeySpec keySpec = cachedKey(keyArg.toString(), operation);

        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(cipherText);
        } catch (IllegalArgumentException e) {
            throw CommonError.illegalArgument("value", operation + ": value is not valid Base64");
        }

        boolean ivProvided = args.size() == 3;
        byte[] iv;
        byte[] encrypted;
        if (ivProvided) {
            iv = resolveIv(args, operation, false);
            encrypted = decoded;
            if (encrypted.length == 0) {
                throw CommonError.illegalArgument(
                        String.valueOf(decoded.length), operation + ": ciphertext is empty");
            }
        } else {
            if (decoded.length < IV_SIZE) {
                throw CommonError.illegalArgument(
                        String.valueOf(decoded.length),
                        operation + ": ciphertext too short to carry a 16-byte IV");
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
            throw TransformCommonError.encryptionError(
                    operation
                            + ": decryption failed (wrong key, wrong IV, or corrupted ciphertext)",
                    e);
        }
    }

    /**
     * Resolves the IV. When the IV argument is present it must be non-null and 16 bytes; an
     * explicit null IV is rejected so a column of mixed null/non-null IVs cannot silently produce
     * two different ciphertext layouts. When no IV is provided, a random 16-byte IV is generated.
     */
    private static byte[] resolveIv(List<Object> args, String operation, boolean generateIfAbsent) {
        if (args.size() != 3) {
            if (!generateIfAbsent) {
                throw new IllegalStateException("resolveIv called without an IV argument");
            }
            byte[] iv = new byte[IV_SIZE];
            SECURE_RANDOM.nextBytes(iv);
            return iv;
        }
        Object ivArg = args.get(2);
        if (ivArg == null) {
            throw CommonError.illegalArgument(
                    "iv",
                    operation + ": iv must not be null (omit the argument to use a random IV)");
        }
        return buildIv(ivArg.toString(), operation);
    }

    /**
     * Builds an AES key from either a raw Base64 key or a passphrase. See the class Javadoc for the
     * key conventions.
     */
    private static SecretKeySpec buildKey(String key, String operation) {
        if (key == null || key.trim().isEmpty()) {
            throw CommonError.illegalArgument("key", operation + ": key must not be null or blank");
        }
        byte[] keyBytes;
        if (key.startsWith(BASE64_PREFIX)) {
            String base64 = key.substring(BASE64_PREFIX.length()).trim();
            try {
                keyBytes = Base64.getDecoder().decode(base64);
            } catch (IllegalArgumentException e) {
                throw CommonError.illegalArgument(
                        "key", operation + ": base64: key is not valid Base64");
            }
            if (!(keyBytes.length == 16 || keyBytes.length == 24 || keyBytes.length == 32)) {
                throw CommonError.illegalArgument(
                        String.valueOf(keyBytes.length),
                        operation + ": base64: key must be 16, 24 or 32 bytes");
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
            throw CommonError.illegalArgument(
                    String.valueOf(ivBytes.length), operation + ": iv must be 16 bytes");
        }
        return ivBytes;
    }

    /** Rejects array / {@code byte[]} / map inputs that would otherwise be encrypted as garbage. */
    private static void rejectNonScalar(Object value, String operation) {
        if (value.getClass().isArray() || value instanceof Map) {
            throw CommonError.illegalArgument(
                    value.getClass().getName(),
                    operation + ": unsupported input type, value must be a scalar string");
        }
    }

    private static SecretKeySpec cachedKey(String key, String operation) {
        SecretKeySpec cached = KEY_CACHE.get(key);
        if (cached != null) {
            return cached;
        }
        SecretKeySpec derived = buildKey(key, operation);
        SecretKeySpec prior = KEY_CACHE.putIfAbsent(key, derived);
        if (prior != null) {
            return prior;
        }
        if (KEY_CACHE.size() > KEY_CACHE_MAX_SIZE) {
            KEY_CACHE.clear();
            KEY_CACHE.putIfAbsent(key, derived);
        }
        return derived;
    }
}

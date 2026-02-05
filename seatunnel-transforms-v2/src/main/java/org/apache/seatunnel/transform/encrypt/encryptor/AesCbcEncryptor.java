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

import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import com.google.auto.service.AutoService;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;

@AutoService(Encryptor.class)
public class AesCbcEncryptor implements Encryptor {
    public static final String IDENTIFIER = "AES_CBC";

    private static final int IV_SIZE = 16;
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final String ALGORITHM = "AES/CBC/PKCS5Padding";

    private SecretKeySpec keySpec;

    @Override
    public boolean support(String algorithm) {
        return IDENTIFIER.equals(algorithm);
    }

    @Override
    public void init(String key) {
        this.keySpec = buildAesKey(key);
    }

    @Override
    public String encrypt(String plainText) {
        byte[] iv = new byte[IV_SIZE];
        SECURE_RANDOM.nextBytes(iv);

        IvParameterSpec ivSpec = new IvParameterSpec(iv);

        byte[] encrypted;
        try {
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, keySpec, ivSpec);
            encrypted = cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw TransformCommonError.encryptionError("plaintext length:" + plainText.length(), e);
        }

        byte[] encryptedWithIv = new byte[IV_SIZE + encrypted.length];
        System.arraycopy(iv, 0, encryptedWithIv, 0, IV_SIZE);
        System.arraycopy(encrypted, 0, encryptedWithIv, IV_SIZE, encrypted.length);

        return Base64.getEncoder().encodeToString(encryptedWithIv);
    }

    @Override
    public String decrypt(String cipherText) {
        byte[] decoded = Base64.getDecoder().decode(cipherText);
        byte[] iv = new byte[IV_SIZE];
        if (decoded.length < IV_SIZE) {
            throw CommonError.illegalArgument(cipherText, "Invalid encrypted value");
        }
        byte[] encrypted = new byte[decoded.length - IV_SIZE];

        System.arraycopy(decoded, 0, iv, 0, IV_SIZE);
        System.arraycopy(decoded, IV_SIZE, encrypted, 0, encrypted.length);

        IvParameterSpec ivSpec = new IvParameterSpec(iv);

        byte[] original;
        try {
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec);
            original = cipher.doFinal(encrypted);
        } catch (Exception e) {
            throw TransformCommonError.encryptionError(
                    "ciphertext length:" + cipherText.length(), e);
        }

        return new String(original, StandardCharsets.UTF_8);
    }

    private SecretKeySpec buildAesKey(String key) {
        if (key == null || key.trim().isEmpty()) {
            throw CommonError.illegalArgument(key, "Encryption key cannot be null or empty");
        }

        String base64 = key;
        if (key.startsWith("base64:")) {
            base64 = key.substring("base64:".length());
        }
        base64 = base64.trim();

        byte[] keyBytes;
        try {
            keyBytes = Base64.getDecoder().decode(base64);
        } catch (IllegalArgumentException e) {
            throw CommonError.illegalArgument(key, "Invalid Base64 encoding in encryption key");
        }

        if (!(keyBytes.length == 16 || keyBytes.length == 24 || keyBytes.length == 32)) {
            throw CommonError.illegalArgument(
                    key,
                    "Invalid AES key length: "
                            + keyBytes.length
                            + ". Expected 16, 24, or 32 bytes");
        }

        return new SecretKeySpec(keyBytes, "AES");
    }
}

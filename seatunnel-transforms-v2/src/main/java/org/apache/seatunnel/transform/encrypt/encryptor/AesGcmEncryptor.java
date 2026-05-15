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
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;

@AutoService(Encryptor.class)
public class AesGcmEncryptor extends AbstractAesEncryptor {
    public static final String IDENTIFIER = "AES_GCM";

    private static final int IV_SIZE = 12;
    private static final int TAG_BIT_LENGTH = 128;

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final String ALGORITHM = "AES/GCM/NoPadding";

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

        GCMParameterSpec spec = new GCMParameterSpec(TAG_BIT_LENGTH, iv);

        byte[] encrypted;
        try {
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, keySpec, spec);
            encrypted = cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw TransformCommonError.encryptionError("Encryption failed", e);
        }

        byte[] encryptedWithIv = new byte[IV_SIZE + encrypted.length];
        System.arraycopy(iv, 0, encryptedWithIv, 0, IV_SIZE);
        System.arraycopy(encrypted, 0, encryptedWithIv, IV_SIZE, encrypted.length);

        return Base64.getEncoder().encodeToString(encryptedWithIv);
    }

    @Override
    public String decrypt(String cipherText) {
        byte[] decoded = Base64.getDecoder().decode(cipherText);

        if (decoded.length < IV_SIZE + (TAG_BIT_LENGTH / 8)) {
            throw CommonError.illegalArgument(cipherText, "Invalid encrypted value (too short)");
        }

        byte[] iv = new byte[IV_SIZE];
        byte[] encrypted = new byte[decoded.length - IV_SIZE];

        System.arraycopy(decoded, 0, iv, 0, IV_SIZE);
        System.arraycopy(decoded, IV_SIZE, encrypted, 0, encrypted.length);

        GCMParameterSpec spec = new GCMParameterSpec(TAG_BIT_LENGTH, iv);

        byte[] original;
        try {
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, keySpec, spec);
            original = cipher.doFinal(encrypted);
        } catch (Exception e) {
            throw TransformCommonError.encryptionError(
                    "Decryption failed (possible tampering or wrong key)", e);
        }

        return new String(original, StandardCharsets.UTF_8);
    }
}

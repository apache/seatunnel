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

import lombok.extern.slf4j.Slf4j;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import javax.crypto.spec.IvParameterSpec;

import java.security.Key;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;

@Slf4j
public class DESUtil {

    private static final String LEGACY_IV_PARAMETER = "12345678";

    private static final String FORMAT_PREFIX = "STDES1:";

    private static final int IV_LENGTH = 8;

    private static final SecureRandom RANDOM = new SecureRandom();

    private static final String ALGORITHM = "DES";

    private static final String CIPHER_ALGORITHM = "DES/CBC/PKCS5Padding";

    private static final String CHARSET = "utf-8";

    private static Key generateKey(String password) throws Exception {
        DESKeySpec dks = new DESKeySpec(password.getBytes(CHARSET));
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(ALGORITHM);
        return keyFactory.generateSecret(dks);
    }

    public static String encrypt(String password, String data) {
        if (password == null || password.length() < 8) {
            throw new RuntimeException("Encrypt failed, password length must greater than 8");
        }
        if (data == null) return null;
        try {
            Key secretKey = generateKey(password);
            Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
            byte[] ivBytes = new byte[IV_LENGTH];
            RANDOM.nextBytes(ivBytes);
            IvParameterSpec iv = new IvParameterSpec(ivBytes);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, iv);
            byte[] bytes = cipher.doFinal(data.getBytes(CHARSET));

            byte[] payload = new byte[ivBytes.length + bytes.length];
            System.arraycopy(ivBytes, 0, payload, 0, ivBytes.length);
            System.arraycopy(bytes, 0, payload, ivBytes.length, bytes.length);
            return FORMAT_PREFIX + Base64.getEncoder().encodeToString(payload);

        } catch (Exception e) {
            throw new IllegalStateException("Encrypt failed", e);
        }
    }

    public static String decrypt(String password, String data) {
        if (password == null || password.length() < 8) {
            throw new RuntimeException("Encrypt failed, password length must greater than 8");
        }
        if (data == null) return null;
        try {
            Key secretKey = generateKey(password);
            Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
            byte[] ivBytes;
            byte[] ciphertext;
            if (data.startsWith(FORMAT_PREFIX)) {
                byte[] payload = Base64.getDecoder().decode(data.substring(FORMAT_PREFIX.length()));
                if (payload.length < IV_LENGTH * 2
                        || (payload.length - IV_LENGTH) % IV_LENGTH != 0) {
                    throw new IllegalArgumentException("Invalid encrypted payload length");
                }
                ivBytes = Arrays.copyOfRange(payload, 0, IV_LENGTH);
                ciphertext = Arrays.copyOfRange(payload, IV_LENGTH, payload.length);
            } else {
                // Read legacy values only; new encryption always uses a fresh IV.
                ivBytes = LEGACY_IV_PARAMETER.getBytes(CHARSET);
                ciphertext = Base64.getDecoder().decode(data);
            }
            cipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(ivBytes));
            return new String(cipher.doFinal(ciphertext), CHARSET);
        } catch (Exception e) {
            if (data.startsWith(FORMAT_PREFIX)) {
                throw new IllegalArgumentException("Decrypt failed", e);
            }
            log.error("Decrypt failed", e);
            return data;
        }
    }
}

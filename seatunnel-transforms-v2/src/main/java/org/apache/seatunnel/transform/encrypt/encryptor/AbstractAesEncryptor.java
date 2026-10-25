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

import javax.crypto.spec.SecretKeySpec;

import java.util.Base64;

public abstract class AbstractAesEncryptor implements Encryptor {
    protected SecretKeySpec buildAesKey(String key) {
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

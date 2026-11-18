/*
 * Copyright 2012-2020 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
// Adapted from Aerospike Java Client 4.4.17 (client/crypto/bouncycastle/Crypto.java).
// SeaTunnel uses Bouncy Castle Base64 in place of JAXB for Java 11 compatibility.
package com.aerospike.client.util;

import org.bouncycastle.crypto.digests.RIPEMD160Digest;
import org.bouncycastle.util.encoders.Base64;

import com.aerospike.client.Value;
import com.aerospike.client.command.Buffer;

public final class Crypto {
    /**
     * Generate unique server hash value from set name, key type and user defined key. The hash
     * function is RIPEMD-160 (a 160 bit hash).
     */
    public static byte[] computeDigest(String setName, Value key) {
        // This method runs 14% faster using thread local byte array
        // versus creating the buffer each time.
        byte[] buffer = ThreadLocalData.getBuffer();
        int setLength = Buffer.stringToUtf8(setName, buffer, 0);

        buffer[setLength] = (byte) key.getType();
        int keyLength = key.write(buffer, setLength + 1);

        RIPEMD160Digest hash = new RIPEMD160Digest();
        hash.update(buffer, 0, setLength);
        hash.update(buffer, setLength, keyLength + 1);

        byte[] digest = new byte[20];
        hash.doFinal(digest, 0);
        return digest;
    }

    /** Decode base64 bytes into a byte array. */
    public static byte[] decodeBase64(byte[] src, int off, int len) {
        return Base64.decode(src, off, len);
    }

    /** Encode bytes into a base64 encoded string. */
    public static String encodeBase64(byte[] src) {
        return Base64.toBase64String(src);
    }
}

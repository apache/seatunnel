/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.aerospike;

import org.junit.jupiter.api.Test;

import com.aerospike.client.Key;
import com.aerospike.client.util.Crypto;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class AerospikeCryptoTest {
    @Test
    void keyDigestsMatchReleasedConnector() {
        // RIPEMD-160 digests captured from the published 3.1.0 connector's Aerospike client.
        assertDigest("J4citZpRhhiZ9vmK6Ra0Htn30wI=", new Key("test", "seatunnel_sink", "20"));
        assertDigest("Wkj9uEvjcBcnQcov2Dh0jsbOd6Y=", new Key("test", "集合", "clé-记录🔑"));
        assertDigest("n6hUJKV6Quorpcx6k5iA5ai+ams=", new Key("test", "", Long.MIN_VALUE));
        assertDigest("eRu+4C0tG6GWVv0WFqQNwHssBhk=", new Key("test", "x", new byte[] {0, -1, 3}));
    }

    @Test
    void base64SupportsPaddedOutputAndDecodingWithinAnInfoResponse() {
        byte[] value = new byte[] {0, -1, 3, 42};
        assertEquals("AP8DKg==", Crypto.encodeBase64(value));
        byte[] response = "status:AP8D\r\nKg==;next".getBytes(StandardCharsets.US_ASCII);
        assertArrayEquals(value, Crypto.decodeBase64(response, "status:".length(), 10));
    }

    private static void assertDigest(String expected, Key key) {
        assertArrayEquals(Base64.getDecoder().decode(expected), key.digest);
    }
}

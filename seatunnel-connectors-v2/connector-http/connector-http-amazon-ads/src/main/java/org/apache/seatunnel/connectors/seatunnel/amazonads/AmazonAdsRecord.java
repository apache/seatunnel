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
package org.apache.seatunnel.connectors.seatunnel.amazonads;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Keeps a provider record intact while deriving a separate identity and checkpoint fingerprint.
 *
 * <p>Record JSON and compact state digests have separate roles during verified recovery.
 */
final class AmazonAdsRecord {
    final String id;
    final JsonNode value;

    AmazonAdsRecord(String id, JsonNode value) {
        this.id = id;
        this.value = value;
    }

    static String identifier(AmazonAdsResource resource, JsonNode value, String field) {
        JsonNode key = value.path(field);
        if ((!key.isTextual() && !key.isIntegralNumber())
                || key.asText().length() > 512
                || key.asText().isBlank()
                || key.asText().codePoints().anyMatch(Character::isISOControl)) {
            throw resource.fail("record has an invalid " + field);
        }
        return key.asText();
    }

    static String hash(String... data) {
        MessageDigest sha = newDigest();
        for (String field : data) {
            add(sha, field.getBytes(StandardCharsets.UTF_8));
        }
        return finish(sha);
    }

    static MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    static void add(MessageDigest digest, byte[] bytes) {
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    static String finish(MessageDigest sha) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(sha.digest());
    }

    static boolean isHash(String value) {
        return value != null && value.matches("[a-zA-Z0-9_-]{43}");
    }
}

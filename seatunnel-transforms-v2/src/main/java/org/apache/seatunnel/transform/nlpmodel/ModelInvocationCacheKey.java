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

package org.apache.seatunnel.transform.nlpmodel;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Builds a stable cache key for embedding invocation inputs.
 *
 * <p>The key is designed to avoid exposing raw content while still distinguishing provider, model,
 * output configuration, modality/format, and metadata that participate in cache identity.
 */
public final class ModelInvocationCacheKey {

    private static final String PREFIX = "embedding-cache:v1";

    private ModelInvocationCacheKey() {}

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private String provider;
        private String model;
        private final TreeMap<String, String> outputConfigurations = new TreeMap<>();
        private String modality;
        private String format;
        private final TreeMap<String, String> metadata = new TreeMap<>();
        private Object input;

        public Builder provider(String provider) {
            this.provider = provider;
            return this;
        }

        public Builder model(String model) {
            this.model = model;
            return this;
        }

        public Builder dimension(Integer dimension) {
            if (dimension != null) {
                this.outputConfigurations.put("dimension", String.valueOf(dimension));
            }
            return this;
        }

        public Builder outputConfiguration(String name, Object value) {
            if (name != null && value != null) {
                this.outputConfigurations.put(normalizeToken(name), normalizeValue(value));
            }
            return this;
        }

        public Builder modality(String modality) {
            this.modality = modality;
            return this;
        }

        public Builder format(String format) {
            this.format = format;
            return this;
        }

        public Builder metadata(String key, Object value) {
            if (key != null && value != null) {
                this.metadata.put(normalizeToken(key), normalizeValue(value));
            }
            return this;
        }

        public Builder input(Object input) {
            this.input = input;
            return this;
        }

        public String build() {
            StringBuilder key = new StringBuilder(PREFIX);
            appendSection(key, "provider", normalizeToken(provider));
            appendSection(key, "model", normalizeValue(model));
            if (!outputConfigurations.isEmpty()) {
                appendSection(key, "output_config", canonicalize(outputConfigurations));
            }
            appendSection(key, "modality", normalizeToken(modality));
            appendSection(key, "format", normalizeToken(format));
            if (!metadata.isEmpty()) {
                appendSection(key, "metadata_sha256", sha256(canonicalize(metadata)));
            }
            appendSection(key, "input_sha256", digestInput(input));
            return key.toString();
        }
    }

    private static void appendSection(StringBuilder key, String label, String value) {
        if (value == null || value.isEmpty()) {
            return;
        }
        key.append('|').append(label).append('=').append(value);
    }

    private static String canonicalize(Map<String, String> values) {
        StringBuilder canonical = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> entry : values.entrySet()) {
            if (!first) {
                canonical.append(',');
            }
            canonical.append(entry.getKey()).append('=').append(entry.getValue());
            first = false;
        }
        return canonical.toString();
    }

    private static String normalizeToken(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim();
        if (normalized.isEmpty()) {
            return null;
        }
        return normalized.toLowerCase();
    }

    private static String normalizeValue(Object value) {
        if (value == null) {
            return null;
        }
        String normalized = String.valueOf(value).trim();
        return normalized.isEmpty() ? null : normalized;
    }

    private static String digestInput(Object input) {
        MessageDigest digest = newDigest();
        if (input == null) {
            digest.update(new byte[0]);
            return hex(digest.digest());
        }
        if (input instanceof byte[]) {
            digest.update((byte[]) input);
            return hex(digest.digest());
        }
        if (input instanceof ByteBuffer) {
            ByteBuffer buffer = ((ByteBuffer) input).duplicate();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            digest.update(bytes);
            return hex(digest.digest());
        }
        String normalized = normalizeLineEndings(String.valueOf(input));
        digest.update(normalized.getBytes(StandardCharsets.UTF_8));
        return hex(digest.digest());
    }

    private static String normalizeLineEndings(String value) {
        return value.replace("\r\n", "\n").replace('\r', '\n');
    }

    private static MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest is required but unavailable", e);
        }
    }

    private static String sha256(String value) {
        MessageDigest digest = newDigest();
        digest.update(value.getBytes(StandardCharsets.UTF_8));
        return hex(digest.digest());
    }

    private static String hex(byte[] bytes) {
        StringBuilder hex = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            hex.append(Character.forDigit((b >>> 4) & 0x0F, 16));
            hex.append(Character.forDigit(b & 0x0F, 16));
        }
        return hex.toString();
    }
}

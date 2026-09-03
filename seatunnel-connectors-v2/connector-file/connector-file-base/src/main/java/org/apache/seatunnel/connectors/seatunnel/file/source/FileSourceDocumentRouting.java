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

package org.apache.seatunnel.connectors.seatunnel.file.source;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Utilities for stable file-backed document identity and route bucket calculation. */
public final class FileSourceDocumentRouting {

    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    private FileSourceDocumentRouting() {}

    /**
     * Builds the stable file-backed document id used by markdown RAG metadata and split routing.
     *
     * @param sourceUri source URI or path
     * @return stable document id
     */
    public static String buildDocumentId(String sourceUri) {
        return "doc_" + sha256Hex(normalizeSourceUri(sourceUri));
    }

    /**
     * Calculates the deterministic route bucket for a document id.
     *
     * @param documentId stable document id
     * @param routeParallelism planned route parallelism
     * @return bucket in the range [0, routeParallelism)
     */
    public static int routeBucket(String documentId, int routeParallelism) {
        if (routeParallelism <= 0) {
            throw new IllegalArgumentException("routeParallelism must be greater than zero");
        }
        byte[] digest = sha256(documentId == null ? "" : documentId);
        return Math.floorMod(ByteBuffer.wrap(digest).getInt(), routeParallelism);
    }

    /**
     * Normalizes local file URIs to the path form emitted by existing local-file reads.
     *
     * @param sourceUri source URI or path
     * @return normalized source URI
     */
    public static String normalizeSourceUri(String sourceUri) {
        if (sourceUri == null || !sourceUri.startsWith("file:")) {
            return sourceUri;
        }
        try {
            return Paths.get(URI.create(sourceUri)).toString();
        } catch (IllegalArgumentException e) {
            return sourceUri;
        }
    }

    /**
     * Returns the lower-case SHA-256 hexadecimal digest used by document metadata fields.
     *
     * @param value source value to hash
     * @return lower-case SHA-256 hexadecimal digest
     */
    public static String sha256Hex(String value) {
        byte[] bytes = sha256(value == null ? "" : value);
        char[] chars = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int unsigned = bytes[i] & 0xFF;
            chars[i * 2] = HEX_CHARS[unsigned >>> 4];
            chars[i * 2 + 1] = HEX_CHARS[unsigned & 0x0F];
        }
        return new String(chars);
    }

    private static byte[] sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}

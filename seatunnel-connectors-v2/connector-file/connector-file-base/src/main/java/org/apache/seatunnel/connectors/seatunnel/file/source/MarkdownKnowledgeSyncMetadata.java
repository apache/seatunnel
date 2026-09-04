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

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.MetadataColumn;
import org.apache.seatunnel.api.table.catalog.MetadataSchema;
import org.apache.seatunnel.api.table.type.KnowledgeSyncMetadataField;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Connector-local bridge between Markdown RAG rows and Knowledge Sync logical metadata. */
public final class MarkdownKnowledgeSyncMetadata {

    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();
    private static final KnowledgeSyncMetadataField[] BRIDGE_FIELDS = {
        KnowledgeSyncMetadataField.SOURCE_URI,
        KnowledgeSyncMetadataField.DOCUMENT_ID,
        KnowledgeSyncMetadataField.DOCUMENT_HASH,
        KnowledgeSyncMetadataField.CHUNK_HASH
    };

    private MarkdownKnowledgeSyncMetadata() {}

    /**
     * Builds a credential-free logical source identity without changing legacy physical identity.
     *
     * @param sourceUri physical source path or URI
     * @return normalized local path or sanitized hierarchical remote URI
     */
    public static String canonicalizeSourceUri(String sourceUri) {
        if (sourceUri == null || sourceUri.trim().isEmpty()) {
            throw unsafeSourceIdentityException();
        }
        if (isWindowsLocalPath(sourceUri)) {
            return sourceUri;
        }

        URI uri;
        try {
            uri = new URI(sourceUri);
        } catch (URISyntaxException e) {
            if (looksLikeUri(sourceUri)) {
                throw unsafeSourceIdentityException();
            }
            return sourceUri;
        }

        if (uri.getScheme() == null) {
            return sourceUri;
        }
        if ("file".equalsIgnoreCase(uri.getScheme())) {
            try {
                return Paths.get(uri).toString();
            } catch (IllegalArgumentException e) {
                throw unsafeSourceIdentityException();
            }
        }
        if (!uri.isAbsolute() || uri.isOpaque()) {
            throw unsafeSourceIdentityException();
        }

        if (uri.getRawAuthority() == null) {
            int schemeSeparator = sourceUri.indexOf(':');
            if (schemeSeparator < 0 || !sourceUri.regionMatches(schemeSeparator + 1, "///", 0, 3)) {
                throw unsafeSourceIdentityException();
            }
            return uri.getScheme().toLowerCase(Locale.ROOT) + "://" + uri.getRawPath();
        }
        if (uri.getHost() == null) {
            throw unsafeSourceIdentityException();
        }

        String host = uri.getHost().toLowerCase(Locale.ROOT);
        if (host.indexOf(':') >= 0 && !host.startsWith("[")) {
            host = '[' + host + ']';
        }
        StringBuilder canonical =
                new StringBuilder()
                        .append(uri.getScheme().toLowerCase(Locale.ROOT))
                        .append("://")
                        .append(host);
        if (uri.getPort() >= 0) {
            canonical.append(':').append(uri.getPort());
        }
        if (uri.getRawPath() != null) {
            canonical.append(uri.getRawPath());
        }
        return canonical.toString();
    }

    /** Builds the logical document id from an already canonicalized logical source URI. */
    public static String buildDocumentId(String canonicalSourceUri) {
        if (canonicalSourceUri == null || canonicalSourceUri.isEmpty()) {
            throw unsafeSourceIdentityException();
        }
        return "doc_" + FileSourceDocumentRouting.sha256Hex(canonicalSourceUri);
    }

    /** Returns a credential-free source context suitable for connector errors and logs. */
    public static String safeSourceContext(String sourceUri) {
        try {
            return canonicalizeSourceUri(sourceUri);
        } catch (RuntimeException e) {
            return "<redacted>";
        }
    }

    /**
     * Retains the failing call site without carrying a cause message that may contain credentials.
     *
     * @param cause original source-operation failure
     * @return sanitized cause chain containing only original type names and stack traces
     */
    public static RuntimeException copyStackTraceOnly(Throwable cause) {
        RuntimeException sanitizedCause =
                new RuntimeException("Sanitized cause type: " + cause.getClass().getName());
        sanitizedCause.setStackTrace(cause.getStackTrace());
        if (cause.getCause() != null && cause.getCause() != cause) {
            sanitizedCause.initCause(copyStackTraceOnly(cause.getCause()));
        }
        return sanitizedCause;
    }

    /** Creates the SHA-256 digest used while the Markdown input stream is read. */
    public static MessageDigest newSha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    /** Returns a lowercase hexadecimal representation of an already calculated digest. */
    public static String toLowerHex(byte[] digest) {
        char[] chars = new char[digest.length * 2];
        for (int i = 0; i < digest.length; i++) {
            int unsigned = digest[i] & 0xFF;
            chars[i * 2] = HEX_CHARS[unsigned >>> 4];
            chars[i * 2 + 1] = HEX_CHARS[unsigned & 0x0F];
        }
        return new String(chars);
    }

    /** Merges registry-defined Markdown bridge metadata into a catalog table. */
    public static CatalogTable withMetadata(CatalogTable catalogTable) {
        List<Column> mergedColumns = new ArrayList<>(catalogTable.getMetadataSchema().getColumns());
        Map<String, Column> existingColumns = new HashMap<>();
        for (Column column : mergedColumns) {
            existingColumns.putIfAbsent(column.getName(), column);
        }

        for (KnowledgeSyncMetadataField field : BRIDGE_FIELDS) {
            MetadataColumn bridgeColumn = field.toMetadataColumn();
            Column existingColumn = existingColumns.get(field.getName());
            if (existingColumn == null) {
                mergedColumns.add(bridgeColumn);
                existingColumns.put(field.getName(), bridgeColumn);
                continue;
            }
            if (!bridgeColumn.getDataType().equals(existingColumn.getDataType())
                    || bridgeColumn.isNullable() != existingColumn.isNullable()) {
                throw new FileConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "Incompatible Markdown Knowledge Sync metadata column: " + field.getName());
            }
        }

        return CatalogTable.withMetadata(
                catalogTable, MetadataSchema.builder().columns(mergedColumns).build());
    }

    private static boolean looksLikeUri(String sourceUri) {
        int colon = sourceUri.indexOf(':');
        if (colon <= 0 || !Character.isLetter(sourceUri.charAt(0))) {
            return false;
        }
        for (int i = 1; i < colon; i++) {
            char value = sourceUri.charAt(i);
            if (!Character.isLetterOrDigit(value) && value != '+' && value != '-' && value != '.') {
                return false;
            }
        }
        return true;
    }

    private static boolean isWindowsLocalPath(String sourceUri) {
        return sourceUri.length() >= 3
                && Character.isLetter(sourceUri.charAt(0))
                && sourceUri.charAt(1) == ':'
                && (sourceUri.charAt(2) == '\\' || sourceUri.charAt(2) == '/');
    }

    private static FileConnectorException unsafeSourceIdentityException() {
        return new FileConnectorException(
                SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                "Cannot derive a safe logical source identity for Markdown input");
    }
}

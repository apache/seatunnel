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

package org.apache.seatunnel.api.table.type;

import org.apache.seatunnel.api.table.catalog.MetadataColumn;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Standard metadata contract for Knowledge Sync document and chunk identity fields.
 *
 * <p>{@link #DOCUMENT_ID} identifies every document lifecycle event. {@link #DELETED} must be
 * explicitly {@code false} for normal rows or {@code true} for document tombstones. Chunk fields
 * remain nullable because a document tombstone does not represent an individual chunk.
 */
public enum KnowledgeSyncMetadataField {
    DOCUMENT_ID("DocumentId", "document_id", BasicType.STRING_TYPE, false),
    DOCUMENT_HASH("DocumentHash", "document_hash", BasicType.STRING_TYPE, true),
    /**
     * Credential-free stable source URI or path.
     *
     * <p>Producers must remove URI user info, access tokens, signatures, and other transient
     * authentication material before storing this value in row options.
     */
    SOURCE_URI("SourceUri", "source_uri", BasicType.STRING_TYPE, true),
    SOURCE_VERSION("SourceVersion", "source_version", BasicType.STRING_TYPE, true),
    SOURCE_MODIFIED_AT("SourceModifiedAt", "source_modified_at", BasicType.LONG_TYPE, true),
    MIME_TYPE("MimeType", "mime_type", BasicType.STRING_TYPE, true),
    DELETED("Deleted", "deleted", BasicType.BOOLEAN_TYPE, false),
    CHUNK_ID("ChunkId", "chunk_id", BasicType.STRING_TYPE, true),
    CHUNK_HASH("ChunkHash", "chunk_hash", BasicType.STRING_TYPE, true),
    CHUNK_INDEX("ChunkIndex", "chunk_index", BasicType.INT_TYPE, true);

    private static final Set<String> FIELD_NAMES =
            Collections.unmodifiableSet(
                    Stream.of(values())
                            .map(KnowledgeSyncMetadataField::getName)
                            .collect(Collectors.toSet()));

    private final String name;
    private final String physicalName;
    private final SeaTunnelDataType<?> dataType;
    private final boolean nullable;

    KnowledgeSyncMetadataField(
            String name, String physicalName, SeaTunnelDataType<?> dataType, boolean nullable) {
        this.name = name;
        this.physicalName = physicalName;
        this.dataType = dataType;
        this.nullable = nullable;
    }

    /** Returns the logical metadata key stored in row options and metadata schemas. */
    public String getName() {
        return name;
    }

    /** Returns the canonical physical field name used when projecting this metadata. */
    public String getPhysicalName() {
        return physicalName;
    }

    /** Returns the field data type defined by the Knowledge Sync contract. */
    public SeaTunnelDataType<?> getDataType() {
        return dataType;
    }

    /** Returns whether the metadata column may contain {@code null}. */
    public boolean isNullable() {
        return nullable;
    }

    /** Creates the metadata column declaration for a Knowledge Sync producer schema. */
    public MetadataColumn toMetadataColumn() {
        return MetadataColumn.of(name, dataType, (Long) null, nullable, null, null);
    }

    /** Returns whether the name is a logical Knowledge Sync metadata key. */
    public static boolean isKnowledgeSyncMetadataField(String name) {
        return FIELD_NAMES.contains(name);
    }
}

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

/** Standard metadata contract for Knowledge Sync document and chunk identity fields. */
public enum KnowledgeSyncMetadataField {
    DOCUMENT_ID("DocumentId", "document_id", BasicType.STRING_TYPE),
    DOCUMENT_HASH("DocumentHash", "document_hash", BasicType.STRING_TYPE),
    SOURCE_URI("SourceUri", "source_uri", BasicType.STRING_TYPE),
    SOURCE_VERSION("SourceVersion", "source_version", BasicType.STRING_TYPE),
    SOURCE_MODIFIED_AT("SourceModifiedAt", "source_modified_at", BasicType.LONG_TYPE),
    MIME_TYPE("MimeType", "mime_type", BasicType.STRING_TYPE),
    DELETED("Deleted", "deleted", BasicType.BOOLEAN_TYPE),
    CHUNK_ID("ChunkId", "chunk_id", BasicType.STRING_TYPE),
    CHUNK_HASH("ChunkHash", "chunk_hash", BasicType.STRING_TYPE),
    CHUNK_INDEX("ChunkIndex", "chunk_index", BasicType.INT_TYPE);

    private final String name;
    private final String physicalName;
    private final SeaTunnelDataType<?> dataType;

    KnowledgeSyncMetadataField(String name, String physicalName, SeaTunnelDataType<?> dataType) {
        this.name = name;
        this.physicalName = physicalName;
        this.dataType = dataType;
    }

    public String getName() {
        return name;
    }

    public String getPhysicalName() {
        return physicalName;
    }

    public SeaTunnelDataType<?> getDataType() {
        return dataType;
    }

    public MetadataColumn toMetadataColumn() {
        return MetadataColumn.of(name, dataType, (Long) null, true, null, null);
    }

    public static boolean isKnowledgeSyncMetadataField(String name) {
        for (KnowledgeSyncMetadataField field : values()) {
            if (field.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }
}

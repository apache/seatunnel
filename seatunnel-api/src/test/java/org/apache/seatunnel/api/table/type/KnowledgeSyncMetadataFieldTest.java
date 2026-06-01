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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class KnowledgeSyncMetadataFieldTest {

    @Test
    void shouldExposeAllKnowledgeSyncMetadataKeys() {
        Set<String> fieldNames =
                Arrays.stream(KnowledgeSyncMetadataField.values())
                        .map(KnowledgeSyncMetadataField::getName)
                        .collect(Collectors.toSet());

        Assertions.assertTrue(fieldNames.contains("DocumentId"));
        Assertions.assertTrue(fieldNames.contains("DocumentHash"));
        Assertions.assertTrue(fieldNames.contains("SourceUri"));
        Assertions.assertTrue(fieldNames.contains("SourceVersion"));
        Assertions.assertTrue(fieldNames.contains("SourceModifiedAt"));
        Assertions.assertTrue(fieldNames.contains("MimeType"));
        Assertions.assertTrue(fieldNames.contains("Deleted"));
        Assertions.assertTrue(fieldNames.contains("ChunkId"));
        Assertions.assertTrue(fieldNames.contains("ChunkHash"));
        Assertions.assertTrue(fieldNames.contains("ChunkIndex"));
    }

    @Test
    void shouldMapLogicalKeysToCanonicalPhysicalFields() {
        Assertions.assertEquals(
                "document_id", KnowledgeSyncMetadataField.DOCUMENT_ID.getPhysicalName());
        Assertions.assertEquals(
                "document_hash", KnowledgeSyncMetadataField.DOCUMENT_HASH.getPhysicalName());
        Assertions.assertEquals(
                "source_uri", KnowledgeSyncMetadataField.SOURCE_URI.getPhysicalName());
        Assertions.assertEquals(
                "source_version", KnowledgeSyncMetadataField.SOURCE_VERSION.getPhysicalName());
        Assertions.assertEquals(
                "source_modified_at",
                KnowledgeSyncMetadataField.SOURCE_MODIFIED_AT.getPhysicalName());
        Assertions.assertEquals(
                "mime_type", KnowledgeSyncMetadataField.MIME_TYPE.getPhysicalName());
        Assertions.assertEquals("deleted", KnowledgeSyncMetadataField.DELETED.getPhysicalName());
        Assertions.assertEquals("chunk_id", KnowledgeSyncMetadataField.CHUNK_ID.getPhysicalName());
        Assertions.assertEquals(
                "chunk_hash", KnowledgeSyncMetadataField.CHUNK_HASH.getPhysicalName());
        Assertions.assertEquals(
                "chunk_index", KnowledgeSyncMetadataField.CHUNK_INDEX.getPhysicalName());
    }

    @Test
    void shouldExposePortableFieldTypes() {
        Assertions.assertEquals(
                BasicType.STRING_TYPE, KnowledgeSyncMetadataField.DOCUMENT_ID.getDataType());
        Assertions.assertEquals(
                BasicType.STRING_TYPE, KnowledgeSyncMetadataField.DOCUMENT_HASH.getDataType());
        Assertions.assertEquals(
                BasicType.STRING_TYPE, KnowledgeSyncMetadataField.SOURCE_URI.getDataType());
        Assertions.assertEquals(
                BasicType.STRING_TYPE, KnowledgeSyncMetadataField.SOURCE_VERSION.getDataType());
        Assertions.assertEquals(
                BasicType.LONG_TYPE, KnowledgeSyncMetadataField.SOURCE_MODIFIED_AT.getDataType());
        Assertions.assertEquals(
                BasicType.STRING_TYPE, KnowledgeSyncMetadataField.MIME_TYPE.getDataType());
        Assertions.assertEquals(
                BasicType.BOOLEAN_TYPE, KnowledgeSyncMetadataField.DELETED.getDataType());
        Assertions.assertEquals(
                BasicType.STRING_TYPE, KnowledgeSyncMetadataField.CHUNK_ID.getDataType());
        Assertions.assertEquals(
                BasicType.STRING_TYPE, KnowledgeSyncMetadataField.CHUNK_HASH.getDataType());
        Assertions.assertEquals(
                BasicType.INT_TYPE, KnowledgeSyncMetadataField.CHUNK_INDEX.getDataType());
    }

    @Test
    void shouldResolveByLogicalKey() {
        Assertions.assertEquals(
                KnowledgeSyncMetadataField.DOCUMENT_ID,
                KnowledgeSyncMetadataField.fromName("DocumentId"));
        Assertions.assertEquals(
                KnowledgeSyncMetadataField.CHUNK_HASH,
                KnowledgeSyncMetadataField.fromName("ChunkHash"));
    }

    @Test
    void shouldRecognizeKnowledgeSyncAndExistingCommonMetadataFields() {
        Assertions.assertTrue(MetadataUtil.isMetadataField("DocumentId"));
        Assertions.assertTrue(MetadataUtil.isMetadataField("ChunkHash"));
        Assertions.assertTrue(MetadataUtil.isMetadataField(CommonOptions.PARTITION.getName()));
        Assertions.assertFalse(MetadataUtil.isMetadataField("UnknownKnowledgeField"));
    }
}

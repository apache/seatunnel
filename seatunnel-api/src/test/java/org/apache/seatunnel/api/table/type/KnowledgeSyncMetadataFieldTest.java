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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class KnowledgeSyncMetadataFieldTest {

    @Test
    void shouldRegisterKnowledgeSyncMetadataFields() {
        Assertions.assertTrue(MetadataUtil.isMetadataField("DocumentId"));
        Assertions.assertTrue(MetadataUtil.isMetadataField("DocumentHash"));
        Assertions.assertTrue(MetadataUtil.isMetadataField("SourceUri"));
        Assertions.assertTrue(MetadataUtil.isMetadataField("SourceVersion"));
        Assertions.assertTrue(MetadataUtil.isMetadataField("SourceModifiedAt"));
        Assertions.assertTrue(MetadataUtil.isMetadataField("MimeType"));
        Assertions.assertTrue(MetadataUtil.isMetadataField("Deleted"));
        Assertions.assertTrue(MetadataUtil.isMetadataField("ChunkId"));
        Assertions.assertTrue(MetadataUtil.isMetadataField("ChunkHash"));
        Assertions.assertTrue(MetadataUtil.isMetadataField("ChunkIndex"));
    }

    @Test
    void shouldRecognizeKnowledgeSyncMetadataFieldNames() {
        for (KnowledgeSyncMetadataField field : KnowledgeSyncMetadataField.values()) {
            Assertions.assertTrue(
                    KnowledgeSyncMetadataField.isKnowledgeSyncMetadataField(field.getName()));
        }
        Assertions.assertFalse(
                KnowledgeSyncMetadataField.isKnowledgeSyncMetadataField("UnknownMetadata"));
        Assertions.assertFalse(KnowledgeSyncMetadataField.isKnowledgeSyncMetadataField(null));
    }

    @Test
    void shouldExposeCanonicalPhysicalNamesAndTypes() {
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
    void shouldCreateMetadataColumnsForProducerSchemas() {
        MetadataColumn column = KnowledgeSyncMetadataField.DOCUMENT_ID.toMetadataColumn();

        Assertions.assertEquals("DocumentId", column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertFalse(column.isNullable());
        Assertions.assertFalse(column.isPhysical());
    }

    @Test
    void shouldDefineLifecycleNullability() {
        Assertions.assertFalse(
                KnowledgeSyncMetadataField.DOCUMENT_ID.toMetadataColumn().isNullable());
        Assertions.assertFalse(KnowledgeSyncMetadataField.DELETED.toMetadataColumn().isNullable());
        Assertions.assertTrue(KnowledgeSyncMetadataField.CHUNK_ID.toMetadataColumn().isNullable());
        Assertions.assertTrue(
                KnowledgeSyncMetadataField.CHUNK_HASH.toMetadataColumn().isNullable());
        Assertions.assertTrue(
                KnowledgeSyncMetadataField.CHUNK_INDEX.toMetadataColumn().isNullable());
    }
}

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
import org.apache.seatunnel.api.table.catalog.MetadataColumn;
import org.apache.seatunnel.api.table.catalog.MetadataSchema;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.KnowledgeSyncMetadataField;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

class MarkdownKnowledgeSyncMetadataTest {

    @TempDir private Path tempDir;

    @Test
    void shouldCanonicalizeLocalAndRemoteSourceUris() {
        Path localPath = tempDir.resolve("document.md");

        Assertions.assertEquals(
                localPath.toString(),
                MarkdownKnowledgeSyncMetadata.canonicalizeSourceUri(localPath.toString()));
        Assertions.assertEquals(
                localPath.toString(),
                MarkdownKnowledgeSyncMetadata.canonicalizeSourceUri(localPath.toUri().toString()));
        Assertions.assertEquals(
                "https://example.com:8443/docs/a%20b.md",
                MarkdownKnowledgeSyncMetadata.canonicalizeSourceUri(
                        "HTTPS://user:secret@Example.COM:8443/docs/a%20b.md"
                                + "?X-Amz-Signature=value#part"));
        Assertions.assertEquals(
                "s3a://bucket/docs/a.md",
                MarkdownKnowledgeSyncMetadata.canonicalizeSourceUri(
                        "s3a://bucket/docs/a.md?versionId=temporary#section"));
        Assertions.assertEquals(
                "hdfs:///docs/a.md",
                MarkdownKnowledgeSyncMetadata.canonicalizeSourceUri(
                        "HDFS:///docs/a.md?temporary=value#section"));
    }

    @Test
    void shouldKeepLogicalIdentityStableAcrossSignedUrlsWithoutChangingLegacyIdentity() {
        String first = "https://first:secret@example.com/docs/a.md?X-Amz-Signature=first#one";
        String second = "https://second:secret@example.com/docs/a.md?X-Amz-Signature=second#two";

        String firstLogicalUri = MarkdownKnowledgeSyncMetadata.canonicalizeSourceUri(first);
        String secondLogicalUri = MarkdownKnowledgeSyncMetadata.canonicalizeSourceUri(second);

        Assertions.assertEquals(firstLogicalUri, secondLogicalUri);
        Assertions.assertEquals(
                MarkdownKnowledgeSyncMetadata.buildDocumentId(firstLogicalUri),
                MarkdownKnowledgeSyncMetadata.buildDocumentId(secondLogicalUri));
        Assertions.assertNotEquals(
                FileSourceDocumentRouting.buildDocumentId(first),
                FileSourceDocumentRouting.buildDocumentId(second));
        Assertions.assertNotEquals(
                FileSourceDocumentRouting.normalizeSourceUri(first), firstLogicalUri);
        Assertions.assertNotEquals(
                FileSourceDocumentRouting.buildDocumentId(first),
                MarkdownKnowledgeSyncMetadata.buildDocumentId(firstLogicalUri));
    }

    @Test
    void shouldRejectUnsafeIdentityWithoutEchoingIt() {
        List<String> unsafeValues =
                Arrays.asList(
                        null,
                        "",
                        "   ",
                        "https://user:secret@example.com/%zz?X-Amz-Signature=value#part");

        for (String unsafeValue : unsafeValues) {
            SeaTunnelRuntimeException exception =
                    Assertions.assertThrows(
                            SeaTunnelRuntimeException.class,
                            () -> MarkdownKnowledgeSyncMetadata.canonicalizeSourceUri(unsafeValue));
            Assertions.assertEquals(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    exception.getSeaTunnelErrorCode());
            Assertions.assertFalse(exception.getMessage().contains("secret"));
            Assertions.assertFalse(exception.getMessage().contains("X-Amz-Signature"));
            Assertions.assertFalse(exception.getMessage().contains("value"));
            Assertions.assertFalse(exception.getMessage().contains("part"));
        }
    }

    @Test
    void shouldMergeRegistryMetadataWithoutReplacingExistingColumns() {
        MetadataColumn existing =
                MetadataColumn.of(
                        "ExistingMetadata", BasicType.LONG_TYPE, (Long) null, true, null, null);
        MetadataColumn compatibleDocumentId =
                KnowledgeSyncMetadataField.DOCUMENT_ID.toMetadataColumn();
        CatalogTable input = catalogTable(existing, compatibleDocumentId);

        CatalogTable merged = MarkdownKnowledgeSyncMetadata.withMetadata(input);

        List<org.apache.seatunnel.api.table.catalog.Column> columns =
                merged.getMetadataSchema().getColumns();
        Assertions.assertEquals(5, columns.size());
        Assertions.assertSame(existing, columns.get(0));
        Assertions.assertSame(compatibleDocumentId, columns.get(1));
        Assertions.assertEquals(
                KnowledgeSyncMetadataField.SOURCE_URI.getName(), columns.get(2).getName());
        Assertions.assertEquals(
                KnowledgeSyncMetadataField.DOCUMENT_HASH.getName(), columns.get(3).getName());
        Assertions.assertEquals(
                KnowledgeSyncMetadataField.CHUNK_HASH.getName(), columns.get(4).getName());
    }

    @Test
    void shouldRejectIncompatibleRegistryMetadata() {
        MetadataColumn incompatible =
                MetadataColumn.of(
                        KnowledgeSyncMetadataField.SOURCE_URI.getName(),
                        BasicType.INT_TYPE,
                        (Long) null,
                        false,
                        null,
                        null);

        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                MarkdownKnowledgeSyncMetadata.withMetadata(
                                        catalogTable(incompatible)));

        Assertions.assertTrue(
                exception.getMessage().contains(KnowledgeSyncMetadataField.SOURCE_URI.getName()));
    }

    private static CatalogTable catalogTable(MetadataColumn... metadataColumns) {
        return CatalogTable.of(
                TableIdentifier.of("catalog", TablePath.DEFAULT),
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "text",
                                        BasicType.STRING_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        null))
                        .build(),
                new HashMap<>(),
                new ArrayList<>(),
                "comment",
                "catalog",
                MetadataSchema.builder()
                        .columns(
                                metadataColumns.length == 0
                                        ? Collections.emptyList()
                                        : Arrays.asList(metadataColumns))
                        .build());
    }
}

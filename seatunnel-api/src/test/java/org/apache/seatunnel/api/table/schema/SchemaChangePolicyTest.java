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

package org.apache.seatunnel.api.table.schema;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterColumnCommentEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableCommentEvent;
import org.apache.seatunnel.api.table.schema.event.RestoreTableSchemaEvent;
import org.apache.seatunnel.api.table.schema.exception.SchemaChangePolicyException;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.exception.NonRetryableException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Protects live DDL policy decisions and replay of already accepted restoration metadata.
 *
 * <p>Coverage distinguishes restoration from new DDL and rejects unsafe mixed-event batches even
 * when one child is metadata-only.
 */
public class SchemaChangePolicyTest {

    @Test
    void testRestoreSchemaReplaysAcceptedMetadataForEveryBehavior() {
        RestoreTableSchemaEvent event =
                new RestoreTableSchemaEvent(
                        CatalogTable.of(
                                TableIdentifier.of("catalog", "database", "table"),
                                TableSchema.builder()
                                        .column(
                                                PhysicalColumn.of(
                                                        "id",
                                                        BasicType.LONG_TYPE,
                                                        20L,
                                                        false,
                                                        null,
                                                        null))
                                        .build(),
                                Collections.emptyMap(),
                                Collections.emptyList(),
                                null));
        for (SchemaChangeBehavior behavior : SchemaChangeBehavior.values()) {
            SchemaChangePolicy.validateStrict(behavior, event, "test-job");
            SchemaChangePolicy.validateIgnore(behavior, event, "test-job");
            Assertions.assertFalse(SchemaChangePolicy.shouldIgnore(behavior, event));
            Assertions.assertTrue(SchemaChangePolicy.isSupported(event, Collections.emptyList()));
            SchemaChangePolicy.validateSupported(event, Collections.emptyList(), "test-job");
        }
    }

    @Test
    void commentOnlyBatchesAreIgnoredButMixedRowLayoutBatchesFail() {
        TableIdentifier table = TableIdentifier.of("catalog", "database", "table");
        AlterColumnCommentEvent comment = AlterColumnCommentEvent.of(table, "id", "old", "new");
        AlterTableColumnsEvent metadata =
                new AlterTableColumnsEvent(table, Collections.singletonList(comment));
        SchemaChangePolicy.validateIgnore(SchemaChangeBehavior.IGNORE, metadata, "test-job");
        Assertions.assertTrue(SchemaChangePolicy.isSafeToIgnore(metadata));
        AlterTableColumnsEvent mixed =
                new AlterTableColumnsEvent(
                        table,
                        Arrays.asList(
                                comment,
                                AlterTableAddColumnEvent.add(
                                        table,
                                        PhysicalColumn.of(
                                                "extra",
                                                BasicType.STRING_TYPE,
                                                20L,
                                                true,
                                                null,
                                                null))));
        Assertions.assertFalse(SchemaChangePolicy.isSafeToIgnore(mixed));
        Assertions.assertThrows(
                SchemaChangePolicyException.class,
                () ->
                        SchemaChangePolicy.validateIgnore(
                                SchemaChangeBehavior.IGNORE, mixed, "test-job"));
    }

    @Test
    void testStrictBehaviorReportsPolicyContract() {
        AlterTableCommentEvent event =
                AlterTableCommentEvent.of(
                        TableIdentifier.of("catalog", "database", "table"),
                        "old comment",
                        "new comment");

        SchemaChangePolicyException exception =
                Assertions.assertThrows(
                        SchemaChangePolicyException.class,
                        () ->
                                SchemaChangePolicy.validateStrict(
                                        SchemaChangeBehavior.STRICT, event, "test-job"));

        Assertions.assertTrue(exception.getMessage().contains("Schema change behavior is STRICT"));
    }

    @Test
    void testUnsupportedAdvertisedCapabilityIsNonRetryable() {
        TableIdentifier tableIdentifier = TableIdentifier.of("catalog", "database", "table");
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        tableIdentifier,
                        PhysicalColumn.of(
                                "added_col", BasicType.STRING_TYPE, 64L, true, null, null));

        SchemaChangePolicyException exception =
                Assertions.assertThrows(
                        SchemaChangePolicyException.class,
                        () ->
                                SchemaChangePolicy.validateSupported(
                                        event, Collections.emptyList(), "test-job"));

        Assertions.assertInstanceOf(NonRetryableException.class, exception);
        Assertions.assertTrue(exception.getMessage().contains("not supported end to end"));
    }
}

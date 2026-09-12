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

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableCommentEvent;
import org.apache.seatunnel.api.table.schema.exception.SchemaChangePolicyException;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.exception.NonRetryableException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SchemaChangePolicyTest {

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
    void testSinkWithoutSchemaEvolutionSupportIsNonRetryable() {
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
                                SchemaChangePolicy.validateSinkSupportsSchemaEvolution(
                                        false, event, "plain", "test-job"));

        Assertions.assertInstanceOf(NonRetryableException.class, exception);
        Assertions.assertTrue(
                exception.getMessage().contains("does not advertise schema evolution"));
    }
}

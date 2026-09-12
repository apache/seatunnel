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

package org.apache.seatunnel.connectors.seatunnel.databend.schema;

import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.AlterColumnCommentEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class SchemaChangeManagerTest {

    @Test
    void testNestedColumnCommentEventIsIgnored() {
        TableIdentifier tableIdentifier = TableIdentifier.of(null, "test_db", "products");
        AlterColumnCommentEvent commentEvent =
                AlterColumnCommentEvent.of(tableIdentifier, "description", null, "Product text");
        AlterTableColumnsEvent groupedEvent =
                new AlterTableColumnsEvent(
                        tableIdentifier, Collections.singletonList(commentEvent));

        SchemaChangeManager manager = new SchemaChangeManager(null);

        assertDoesNotThrow(
                () ->
                        manager.applySchemaChange(
                                null, TablePath.of("test_db", "products"), groupedEvent));
    }
}

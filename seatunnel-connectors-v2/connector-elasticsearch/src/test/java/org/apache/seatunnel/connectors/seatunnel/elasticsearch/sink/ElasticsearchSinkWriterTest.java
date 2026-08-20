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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.sink;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.AlterColumnCommentEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableCommentEvent;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ElasticsearchSinkWriterTest {

    private static final TableIdentifier TABLE_IDENTIFIER =
            TableIdentifier.of("", TablePath.DEFAULT);

    @Test
    void commentOnlySchemaChangeEventsAreNoOpForElasticsearch() {
        Assertions.assertTrue(
                ElasticsearchSinkWriter.isCommentOnlyEvent(
                        AlterTableCommentEvent.of(TABLE_IDENTIFIER, "old", "new")));
        Assertions.assertTrue(
                ElasticsearchSinkWriter.isCommentOnlyEvent(
                        AlterColumnCommentEvent.of(TABLE_IDENTIFIER, "name", "old", "new")));
    }

    @Test
    void physicalSchemaChangeEventsStillRequireElasticsearchMappingChanges() {
        Assertions.assertFalse(
                ElasticsearchSinkWriter.isCommentOnlyEvent(
                        AlterTableAddColumnEvent.add(
                                TABLE_IDENTIFIER,
                                PhysicalColumn.builder()
                                        .name("name")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build())));
    }
}

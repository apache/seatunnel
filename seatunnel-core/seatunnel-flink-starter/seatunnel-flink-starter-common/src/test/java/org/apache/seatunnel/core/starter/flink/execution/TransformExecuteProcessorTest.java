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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;
import org.apache.seatunnel.translation.flink.schema.SchemaEvolutionControlMessage;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransformExecuteProcessorTest {

    private static final String FIRST_SCHEMA_CHANGE_ID = "producer-1#1";
    private static final String SECOND_SCHEMA_CHANGE_ID = "producer-1#2";

    @Test
    void testReplacementRowRetainsSchemaDependency() {
        SeaTunnelRow input = new SeaTunnelRow(new Object[] {"before"});
        Map<String, Object> options = new HashMap<>();
        options.put(
                SchemaEvolutionControlMessage.REQUIRED_SCHEMA_CHANGE_ID, FIRST_SCHEMA_CHANGE_ID);
        input.setOptions(options);

        SeaTunnelRow result =
                TransformExecuteProcessor.mapRow(new TestingMapTransform(false), input);

        assertEquals("after", result.getField(0));
        assertEquals(
                FIRST_SCHEMA_CHANGE_ID,
                SchemaEvolutionControlMessage.requiredSchemaChangeId(result));
    }

    @Test
    void testFilteredSchemaEventRetainsDependencyIdAsNoopMarker() {
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        TableIdentifier.of("catalog", "database", "table"),
                        PhysicalColumn.of(
                                "added_col", BasicType.STRING_TYPE, 64L, true, null, null));
        SeaTunnelRow schemaRow = new SeaTunnelRow(0);
        Map<String, Object> options = new HashMap<>();
        options.put(SchemaEvolutionControlMessage.SCHEMA_CHANGE_BROADCAST, event);
        options.put(SchemaEvolutionControlMessage.SCHEMA_CHANGE_ID, SECOND_SCHEMA_CHANGE_ID);
        schemaRow.setOptions(options);

        SeaTunnelRow result =
                TransformExecuteProcessor.mapRow(new TestingMapTransform(true), schemaRow);

        assertTrue(SchemaEvolutionControlMessage.isFilteredSchemaChange(result));
        assertEquals(SECOND_SCHEMA_CHANGE_ID, SchemaEvolutionControlMessage.schemaChangeId(result));
    }

    private static final class TestingMapTransform implements SeaTunnelMapTransform<SeaTunnelRow> {
        private final boolean filterSchemaEvent;

        private TestingMapTransform(boolean filterSchemaEvent) {
            this.filterSchemaEvent = filterSchemaEvent;
        }

        @Override
        public SeaTunnelRow map(SeaTunnelRow row) {
            return new SeaTunnelRow(new Object[] {"after"});
        }

        @Override
        public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent schemaChangeEvent) {
            return filterSchemaEvent ? null : schemaChangeEvent;
        }

        @Override
        public CatalogTable getProducedCatalogTable() {
            return null;
        }

        @Override
        public List<CatalogTable> getProducedCatalogTables() {
            return Collections.emptyList();
        }

        @Override
        public String getPluginName() {
            return "test";
        }
    }
}

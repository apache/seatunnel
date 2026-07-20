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

package org.apache.seatunnel.connectors.cdc.base.source.reader;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.relational.TableId;

import java.util.Collections;
import java.util.List;
import java.util.Map;

class IncrementalSourceReaderTest {

    @Test
    void restoreCheckpointStateRestoresTablesAndHistoryFromNewCheckpointFormat() {
        @SuppressWarnings("unchecked")
        DebeziumDeserializationSchema<Object> schema =
                Mockito.mock(DebeziumDeserializationSchema.class);
        List<CatalogTable> checkpointTables =
                Collections.singletonList(Mockito.mock(CatalogTable.class));
        Map<TableId, byte[]> historyTableChanges =
                Collections.singletonMap(
                        new TableId("catalog", "database", "new_table"), new byte[] {1});
        IncrementalSplit incrementalSplit =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.emptyList(),
                        Mockito.mock(Offset.class),
                        Mockito.mock(Offset.class),
                        Collections.emptyList(),
                        checkpointTables,
                        historyTableChanges);

        IncrementalSourceReader.restoreCheckpointState(incrementalSplit, schema);

        Mockito.verify(schema).restoreCheckpointProducedType(checkpointTables);
        Mockito.verify(schema).restoreCheckpointHistoryTableChanges(historyTableChanges);
    }

    @Test
    void restoreCheckpointStateIgnoresEmptyLegacyState() {
        @SuppressWarnings("unchecked")
        DebeziumDeserializationSchema<Object> schema =
                Mockito.mock(DebeziumDeserializationSchema.class);
        IncrementalSplit incrementalSplit =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.emptyList(),
                        Mockito.mock(Offset.class),
                        Mockito.mock(Offset.class),
                        Collections.emptyList());

        IncrementalSourceReader.restoreCheckpointState(incrementalSplit, schema);

        Mockito.verifyNoInteractions(schema);
    }
}

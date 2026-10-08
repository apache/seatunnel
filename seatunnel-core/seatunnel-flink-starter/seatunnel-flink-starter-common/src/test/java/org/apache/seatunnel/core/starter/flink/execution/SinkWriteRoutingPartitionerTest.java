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

import org.apache.seatunnel.api.sink.SinkDataPartitioner;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class SinkWriteRoutingPartitionerTest {

    @Test
    void shouldExpandOnlySchemaBroadcastsWithoutCopyingDataRows() throws Exception {
        List<SeaTunnelRow> output = new ArrayList<>();
        Collector<SeaTunnelRow> collector =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow row) {
                        output.add(row);
                    }

                    @Override
                    public void close() {}
                };
        SeaTunnelRow broadcast = new SeaTunnelRow(0);
        Map<String, Object> options = new HashMap<>();
        options.put("schema_change_broadcast", mock(SchemaChangeEvent.class));
        broadcast.setOptions(options);
        SinkWriteRoutingPartitioner.SchemaBroadcastExpander expander =
                new SinkWriteRoutingPartitioner.SchemaBroadcastExpander(2);
        expander.flatMap(broadcast, collector);
        assertEquals(2, output.size());
        SinkDataPartitioner<SeaTunnelRow> routing = mock(SinkDataPartitioner.class);
        SinkWriteRoutingPartitioner.RoutingKeySelector selector =
                new SinkWriteRoutingPartitioner.RoutingKeySelector(routing, 2);
        assertEquals(0, selector.getKey(output.get(0)));
        assertEquals(1, selector.getKey(output.get(1)));
        assertFalse(broadcast.getOptions().containsKey("schema_subtask_id"));
        verifyNoInteractions(routing);
        SeaTunnelRow data = new SeaTunnelRow(new Object[] {42});
        expander.flatMap(data, collector);
        assertSame(data, output.get(2));
    }

    @Test
    void shouldKeepOneSchemaControlRowPerSinkSubtask() {
        SinkDataPartitioner<SeaTunnelRow> routing = mock(SinkDataPartitioner.class);
        SinkWriteRoutingPartitioner.RoutingKeySelector selector =
                new SinkWriteRoutingPartitioner.RoutingKeySelector(routing, 2);
        SinkWriteRoutingPartitioner partitioner = new SinkWriteRoutingPartitioner();
        for (long subtask = 0; subtask < 2; subtask++) {
            SeaTunnelRow control = controlRow(subtask);
            assertEquals((int) subtask, partitioner.partition(selector.getKey(control), 2));
            assertEquals(subtask, control.getOptions().get("schema_subtask_id"));
        }
        verifyNoInteractions(routing);
    }

    @Test
    void shouldRejectSchemaControlRowsWithoutValidDestination() {
        SinkDataPartitioner<SeaTunnelRow> routing = mock(SinkDataPartitioner.class);
        SinkWriteRoutingPartitioner.RoutingKeySelector selector =
                new SinkWriteRoutingPartitioner.RoutingKeySelector(routing, 2);
        for (Object destination : new Object[] {null, -1L, 2L, "0"}) {
            assertThrows(
                    IllegalArgumentException.class, () -> selector.getKey(controlRow(destination)));
        }
        verifyNoInteractions(routing);
    }

    @Test
    void shouldApplyOwnershipPolicyToDataRows() {
        SinkDataPartitioner<SeaTunnelRow> routing = mock(SinkDataPartitioner.class);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {42});
        when(routing.select(row)).thenReturn(1);
        SinkWriteRoutingPartitioner.RoutingKeySelector selector =
                new SinkWriteRoutingPartitioner.RoutingKeySelector(routing, 2);
        assertEquals(1, new SinkWriteRoutingPartitioner().partition(selector.getKey(row), 2));
    }

    @Test
    void shouldRejectInvalidWriterOwnership() {
        SinkWriteRoutingPartitioner partitioner = new SinkWriteRoutingPartitioner();
        assertThrows(IllegalStateException.class, () -> partitioner.partition(-1, 2));
        assertThrows(IllegalStateException.class, () -> partitioner.partition(2, 2));
    }

    private SeaTunnelRow controlRow(Object destination) {
        SeaTunnelRow row = new SeaTunnelRow(0);
        Map<String, Object> options = new HashMap<>();
        options.put("schema_change_event", mock(SchemaChangeEvent.class));
        options.put("schema_subtask_id", destination);
        row.setOptions(options);
        return row;
    }
}

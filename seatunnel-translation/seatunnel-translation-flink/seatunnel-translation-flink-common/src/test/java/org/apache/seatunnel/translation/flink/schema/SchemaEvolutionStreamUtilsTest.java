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

package org.apache.seatunnel.translation.flink.schema;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class SchemaEvolutionStreamUtilsTest {

    @Test
    void testDataIsPartitionedByTableWhileSchemaControlIsBroadcast() {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);

        SchemaEvolutionStreamUtils.routeSchemaChanges(
                        environment.fromCollection(
                                Collections.singletonList(new SeaTunnelRow(0)),
                                TypeInformation.of(SeaTunnelRow.class)),
                        3,
                        Collections.emptyList())
                .addSink(new SinkFunction<SeaTunnelRow>() {});

        StreamGraph streamGraph = environment.getStreamGraph();
        StreamNode schemaGate =
                streamGraph.getStreamNodes().stream()
                        .filter(node -> "BroadcastSchemaHandler".equals(node.getOperatorName()))
                        .findFirst()
                        .orElse(null);

        assertNotNull(schemaGate);
        assertEquals(3, schemaGate.getParallelism());
        assertEquals(2, schemaGate.getInEdges().size());
        assertEquals(
                1,
                schemaGate.getInEdges().stream()
                        .map(StreamEdge::getPartitioner)
                        .filter(BroadcastPartitioner.class::isInstance)
                        .count());
        assertEquals(
                1,
                schemaGate.getInEdges().stream()
                        .map(StreamEdge::getPartitioner)
                        .filter(KeyGroupStreamPartitioner.class::isInstance)
                        .count());
    }
}

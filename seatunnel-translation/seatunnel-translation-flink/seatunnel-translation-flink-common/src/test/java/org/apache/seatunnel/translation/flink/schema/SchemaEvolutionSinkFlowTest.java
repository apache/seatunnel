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

import org.apache.seatunnel.api.sink.SchemaChangeApplier;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportCoordinatedSchemaEvolutionSink;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

class SchemaEvolutionSinkFlowTest {

    @Test
    void testCoordinatedFlowUsesEnvironmentParallelismWhenUnconfigured() {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(4);

        DataStream<SeaTunnelRow> coordinated =
                SchemaEvolutionSinkFlow.coordinate(
                        createInput(environment), new TestingCoordinatedSink(), 1, false);
        DataStreamSink<SeaTunnelRow> sink = coordinated.addSink(new DiscardingSink<>());

        Assertions.assertEquals(4, coordinated.getParallelism());
        Assertions.assertEquals(4, sink.getTransformation().getParallelism());
        Assertions.assertDoesNotThrow(() -> environment.getStreamGraph());
    }

    @Test
    void testCoordinatedFlowUsesConfiguredParallelism() {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(4);

        DataStream<SeaTunnelRow> coordinated =
                SchemaEvolutionSinkFlow.coordinate(
                        createInput(environment), new TestingCoordinatedSink(), 2, true);
        DataStreamSink<SeaTunnelRow> sink =
                coordinated.addSink(new DiscardingSink<>()).setParallelism(2);

        Assertions.assertEquals(2, coordinated.getParallelism());
        Assertions.assertEquals(2, sink.getTransformation().getParallelism());
        Assertions.assertDoesNotThrow(() -> environment.getStreamGraph());
    }

    private DataStream<SeaTunnelRow> createInput(StreamExecutionEnvironment environment) {
        return environment.fromCollection(
                Collections.singletonList(new SeaTunnelRow(1)),
                TypeInformation.of(SeaTunnelRow.class));
    }

    private static final class TestingCoordinatedSink
            implements SeaTunnelSink<SeaTunnelRow, String, String, String>,
                    SupportCoordinatedSchemaEvolutionSink {

        @Override
        public SinkWriter<SeaTunnelRow, String, String> createWriter(SinkWriter.Context context) {
            throw new UnsupportedOperationException("The test only builds the Flink stream graph");
        }

        @Override
        public String getPluginName() {
            return "test";
        }

        @Override
        public List<SchemaChangeType> supports() {
            return Collections.emptyList();
        }

        @Override
        public SchemaChangeApplier createSchemaChangeApplier(TablePath sinkTablePath)
                throws IOException {
            return event -> {};
        }
    }
}

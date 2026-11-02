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

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkDataPartitioner;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSink;
import org.apache.seatunnel.api.sink.SupportSinkDataPartition;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.flink.schema.BroadcastSchemaSinkOperator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Applies writer ownership while preserving each schema control message's downstream subtask. */
public final class SinkWriteRoutingPartitioner implements Partitioner<Integer> {

    private static final long serialVersionUID = 1L;

    /**
     * Routes before the schema operator can introduce a rebalance. Normal records retain their
     * source-channel order. Only schema broadcasts are expanded, once per downstream writer;
     * BroadcastSchemaSinkOperator deduplicates repeated epochs and emits targeted control rows.
     */
    public static DataStream<SeaTunnelRow> prepare(
            DataStream<SeaTunnelRow> stream,
            SeaTunnelSink<SeaTunnelRow, ?, ?, ?> sink,
            int parallelism,
            boolean streaming) {
        Optional<SinkDataPartitioner<SeaTunnelRow>> routing =
                SupportSinkDataPartition.resolve(sink, parallelism);
        boolean schemaEvolutionEnabled = streaming && sink instanceof SupportSchemaEvolutionSink;
        if (routing.isPresent()) {
            if (schemaEvolutionEnabled) {
                // Keep this operator at the input parallelism: no rebalance before bucket routing.
                stream =
                        stream.forward()
                                .flatMap(new SchemaBroadcastExpander(parallelism))
                                .returns(TypeInformation.of(SeaTunnelRow.class))
                                .name("SinkSchemaBroadcastExpander")
                                .setParallelism(stream.getParallelism());
            }
            stream =
                    stream.partitionCustom(
                            new SinkWriteRoutingPartitioner(),
                            new RoutingKeySelector(routing.get(), parallelism));
        }
        if (schemaEvolutionEnabled) {
            stream =
                    stream.transform(
                                    "BroadcastSchemaHandler",
                                    TypeInformation.of(SeaTunnelRow.class),
                                    new BroadcastSchemaSinkOperator())
                            .name("BroadcastSchemaHandler")
                            .setParallelism(parallelism);
        }
        return stream;
    }

    static final class SchemaBroadcastExpander
            implements FlatMapFunction<SeaTunnelRow, SeaTunnelRow> {
        private static final long serialVersionUID = 1L;
        private final int parallelism;

        SchemaBroadcastExpander(int parallelism) {
            this.parallelism = parallelism;
        }

        @Override
        public void flatMap(SeaTunnelRow row, Collector<SeaTunnelRow> output) {
            if (row.getOptions() != null
                    && row.getOptions().containsKey("schema_change_broadcast")) {
                for (long destination = 0; destination < parallelism; destination++) {
                    SeaTunnelRow control = new SeaTunnelRow(0);
                    Map<String, Object> options = new HashMap<>(row.getOptions());
                    options.put("schema_subtask_id", destination);
                    control.setOptions(options);
                    output.collect(control);
                }
            } else {
                output.collect(row);
            }
        }
    }

    @Override
    public int partition(Integer writer, int numberOfPartitions) {
        if (writer == null || writer < 0 || writer >= numberOfPartitions) {
            throw new IllegalStateException(
                    "Sink routing returned an invalid writer index: " + writer);
        }
        return writer;
    }

    public static final class RoutingKeySelector implements KeySelector<SeaTunnelRow, Integer> {

        private static final long serialVersionUID = 1L;

        private final SinkDataPartitioner<SeaTunnelRow> routing;
        private final int parallelism;

        public RoutingKeySelector(SinkDataPartitioner<SeaTunnelRow> routing, int parallelism) {
            if (parallelism <= 0) {
                throw new IllegalArgumentException("Sink writer parallelism must be positive");
            }
            this.routing = routing;
            this.parallelism = parallelism;
        }

        @Override
        public Integer getKey(SeaTunnelRow row) {
            Map<String, Object> options = row.getOptions();
            if (options != null
                    && (options.containsKey("schema_change_event")
                            || options.containsKey("schema_change_broadcast"))) {
                // BroadcastSchemaSinkOperator emits zero-field rows, one for each sink subtask.
                // Preserve that destination so the writer can apply the event and acknowledge it.
                Object destination = options.get("schema_subtask_id");
                if (!(destination instanceof Long)
                        || (Long) destination < 0
                        || (Long) destination >= parallelism) {
                    throw new IllegalArgumentException(
                            "Schema control row has an invalid schema_subtask_id: " + destination);
                }
                return ((Long) destination).intValue();
            }
            return routing.select(row);
        }
    }
}

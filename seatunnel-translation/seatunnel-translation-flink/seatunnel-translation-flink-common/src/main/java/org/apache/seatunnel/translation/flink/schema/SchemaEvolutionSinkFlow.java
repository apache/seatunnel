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

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SupportCoordinatedSchemaEvolutionSink;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Map;

/** Builds the ordered external-apply and writer-refresh flow for Flink schema evolution. */
public final class SchemaEvolutionSinkFlow {

    private SchemaEvolutionSinkFlow() {}

    public static DataStream<SeaTunnelRow> coordinate(
            DataStream<SeaTunnelRow> input,
            SeaTunnelSink<?, ?, ?, ?> sink,
            int sinkParallelism,
            boolean parallelismConfigured) {
        if (!(sink instanceof SupportCoordinatedSchemaEvolutionSink)
                || !((SupportCoordinatedSchemaEvolutionSink) sink)
                        .supportsCoordinatedSchemaEvolution()) {
            SingleOutputStreamOperator<SeaTunnelRow> broadcastSchemaHandler =
                    input.transform(
                                    "BroadcastSchemaHandler",
                                    TypeInformation.of(SeaTunnelRow.class),
                                    new BroadcastSchemaSinkOperator())
                            .name("BroadcastSchemaHandler");
            if (parallelismConfigured) {
                broadcastSchemaHandler.setParallelism(sinkParallelism);
            }
            return broadcastSchemaHandler;
        }

        DataStream<SeaTunnelRow> dataRows =
                input.filter(SchemaEvolutionSinkFlow::isDataRow).name("SchemaEvolutionDataRows");
        DataStream<SeaTunnelRow> schemaRows =
                input.filter(SchemaEvolutionSinkFlow::isSchemaChangeRow)
                        .name("SchemaEvolutionControlRows")
                        .transform(
                                "ExternalSchemaChangeApplier",
                                TypeInformation.of(SeaTunnelRow.class),
                                new ExternalSchemaChangeOperator(sink))
                        .name("ExternalSchemaChangeApplier")
                        .setParallelism(1);
        SingleOutputStreamOperator<SeaTunnelRow> schemaRefreshBarrier =
                dataRows.connect(schemaRows.broadcast())
                        .transform(
                                "SchemaRefreshBarrier",
                                TypeInformation.of(SeaTunnelRow.class),
                                new SchemaRefreshBarrierOperator())
                        .name("SchemaRefreshBarrier");
        if (parallelismConfigured) {
            schemaRefreshBarrier.setParallelism(sinkParallelism);
        }
        return schemaRefreshBarrier.forward();
    }

    private static boolean isDataRow(SeaTunnelRow row) {
        return !isSchemaChangeRow(row);
    }

    private static boolean isSchemaChangeRow(SeaTunnelRow row) {
        Map<String, Object> options = row.getOptions();
        return options != null && options.containsKey("schema_change_broadcast");
    }
}

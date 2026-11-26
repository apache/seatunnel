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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Builds the Flink data-plane routing used for schema change control records. */
public final class SchemaEvolutionStreamUtils {

    private SchemaEvolutionStreamUtils() {}

    /** Routes table data and schema progress through the same keyed channel. */
    public static SingleOutputStreamOperator<SeaTunnelRow> routeSchemaChanges(
            DataStream<SeaTunnelRow> input,
            int sinkParallelism,
            List<CatalogTable> initialSinkTables) {
        return input.flatMap(new SchemaTableRouter(initialSinkTables))
                .name("SchemaEvolutionTableRouting")
                .keyBy(new TableIdKeySelector())
                .transform(
                        "BroadcastSchemaHandler",
                        TypeInformation.of(SeaTunnelRow.class),
                        new BroadcastSchemaSinkOperator(initialSinkTables))
                .name("BroadcastSchemaHandler")
                .setParallelism(sinkParallelism);
    }

    static final class SchemaTableRouter implements FlatMapFunction<SeaTunnelRow, SeaTunnelRow> {
        private static final long serialVersionUID = 1L;
        private final Set<String> tableIds = new LinkedHashSet<>();

        SchemaTableRouter(List<CatalogTable> tables) {
            for (CatalogTable table : tables) {
                tableIds.add(table.getTablePath().toString());
            }
        }

        @Override
        public void flatMap(SeaTunnelRow row, Collector<SeaTunnelRow> out) {
            if (!SchemaEvolutionControlMessage.isSchemaBroadcast(row)) {
                if (!tableIds.contains(row.getTableId())) {
                    throw new IllegalArgumentException(
                            "Schema evolution has no initial sink schema for table "
                                    + row.getTableId());
                }
                out.collect(row);
                return;
            }
            for (String tableId : tableIds) {
                SeaTunnelRow control = new SeaTunnelRow(0);
                control.setTableId(tableId);
                control.setOptions(new HashMap<>(row.getOptions()));
                out.collect(control);
            }
        }
    }

    private static final class TableIdKeySelector implements KeySelector<SeaTunnelRow, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String getKey(SeaTunnelRow row) {
            String tableId = row.getTableId();
            if (tableId == null || tableId.isEmpty()) {
                throw new IllegalArgumentException(
                        "Schema evolution requires every data row to carry a table identifier");
            }
            return tableId;
        }
    }
}

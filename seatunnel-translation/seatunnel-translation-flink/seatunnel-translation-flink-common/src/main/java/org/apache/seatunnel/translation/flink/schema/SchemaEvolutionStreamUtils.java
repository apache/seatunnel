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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.List;

/** Builds the Flink data-plane routing used for schema change control records. */
public final class SchemaEvolutionStreamUtils {

    private SchemaEvolutionStreamUtils() {}

    /**
     * Routes all rows for one table to one sink gate and broadcasts only schema control records.
     *
     * <p>The table partition prevents multiple sink writers from applying physical DDL for the same
     * table. The lightweight schema broadcast lets every gate advance the source sequence and
     * release rows that carry a global schema dependency. If an unaligned checkpoint or the two
     * network branches reorder a row and its schema control, the sink gate buffers the row until
     * the control arrives.
     */
    public static SingleOutputStreamOperator<SeaTunnelRow> routeSchemaChanges(
            DataStream<SeaTunnelRow> input,
            int sinkParallelism,
            List<CatalogTable> initialSinkTables) {
        DataStream<SeaTunnelRow> dataRows =
                input.filter(new DataRowFilter())
                        .name("SchemaEvolutionDataRows")
                        .keyBy(new TableIdKeySelector());
        DataStream<SeaTunnelRow> schemaRows =
                input.filter(new SchemaRowFilter()).name("SchemaEvolutionControlRows").broadcast();

        return dataRows.union(schemaRows)
                .transform(
                        "BroadcastSchemaHandler",
                        TypeInformation.of(SeaTunnelRow.class),
                        new BroadcastSchemaSinkOperator(initialSinkTables))
                .name("BroadcastSchemaHandler")
                .setParallelism(sinkParallelism);
    }

    private static final class DataRowFilter implements FilterFunction<SeaTunnelRow> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean filter(SeaTunnelRow row) {
            return !SchemaEvolutionControlMessage.isSchemaBroadcast(row);
        }
    }

    private static final class SchemaRowFilter implements FilterFunction<SeaTunnelRow> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean filter(SeaTunnelRow row) {
            return SchemaEvolutionControlMessage.isSchemaBroadcast(row);
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

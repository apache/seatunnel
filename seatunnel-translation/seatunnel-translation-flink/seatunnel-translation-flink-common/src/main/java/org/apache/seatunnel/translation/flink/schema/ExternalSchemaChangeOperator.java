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
import org.apache.seatunnel.api.sink.SupportCoordinatedSchemaEvolutionSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Applies an external schema change once before it is broadcast for writer-local refresh. */
public class ExternalSchemaChangeOperator extends AbstractStreamOperator<SeaTunnelRow>
        implements OneInputStreamOperator<SeaTunnelRow, SeaTunnelRow> {

    private final SeaTunnelSink<?, ?, ?, ?> sink;
    private transient Map<TablePath, SchemaChangeApplier> schemaChangeAppliers;

    public ExternalSchemaChangeOperator(SeaTunnelSink<?, ?, ?, ?> sink) {
        this.sink = sink;
    }

    @Override
    public void open() throws Exception {
        super.open();
        schemaChangeAppliers = new HashMap<>();
    }

    @Override
    public void processElement(StreamRecord<SeaTunnelRow> element) throws Exception {
        SeaTunnelRow row = element.getValue();
        Map<String, Object> options = row.getOptions();
        if (options == null || !options.containsKey("schema_change_broadcast")) {
            return;
        }

        SchemaChangeEvent event = (SchemaChangeEvent) options.get("schema_change_broadcast");
        if (event.getChangeAfter() == null) {
            throw new IllegalStateException(
                    "Coordinated schema evolution requires the complete evolved schema");
        }
        TablePath physicalSinkTable = resolvePhysicalSinkTable(event);
        SchemaChangeApplier applier = schemaChangeAppliers.get(physicalSinkTable);
        if (applier == null) {
            applier =
                    ((SupportCoordinatedSchemaEvolutionSink) sink)
                            .createSchemaChangeApplier(physicalSinkTable);
            schemaChangeAppliers.put(physicalSinkTable, applier);
        }
        applier.apply(event);
        output.collect(element);
    }

    private TablePath resolvePhysicalSinkTable(SchemaChangeEvent event) {
        Optional<CatalogTable> writeCatalogTable = sink.getWriteCatalogTable();
        return writeCatalogTable.isPresent()
                ? writeCatalogTable.get().getTablePath()
                : event.tablePath();
    }

    @Override
    public void close() throws Exception {
        Exception failure = null;
        if (schemaChangeAppliers != null) {
            for (SchemaChangeApplier applier : schemaChangeAppliers.values()) {
                try {
                    applier.close();
                } catch (Exception error) {
                    if (failure == null) {
                        failure = error;
                    } else {
                        failure.addSuppressed(error);
                    }
                }
            }
        }
        try {
            super.close();
        } catch (Exception error) {
            if (failure == null) {
                failure = error;
            } else {
                failure.addSuppressed(error);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}

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

package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Optional;

public interface SupportSchemaEvolutionSinkWriter {

    /**
     * apply schema change to third party data receiver.
     *
     * @param event
     * @throws IOException
     */
    void applySchemaChange(SchemaChangeEvent event) throws IOException;

    /**
     * Returns a stable identifier of the physical sink table this writer commits to. Multi-table
     * sinks that resolve a sink-table template per upstream table can end up with several writers
     * sharing one physical destination. When that happens, a schema change applied through one
     * sub-writer mutates the external table immediately while sibling sub-writers keep writing with
     * their stale in-memory schema unless the coordinator can fan the change out to all of them.
     *
     * <p>Writers that can share one physical destination should expose that resolved identifier
     * here. The default implementation returns {@link Optional#empty()} so connectors that do not
     * need shared-sink coordination keep the legacy source-only routing.
     */
    default Optional<String> getPhysicalSinkTableIdentifier() {
        return Optional.empty();
    }

    /**
     * Applies a schema change only when the sink writer explicitly declares schema evolution
     * support.
     *
     * <p>The deprecated {@link SinkWriter#applySchemaChange(SchemaChangeEvent)} path is kept only
     * for legacy writers that really override it. Writers that inherit the default no-op method
     * must fail fast, otherwise a CDC schema change could be silently dropped while new-schema
     * records continue downstream.
     *
     * @param writer sink writer that receives the schema change event
     * @param event schema change event from upstream
     * @throws IOException if the sink writer fails while applying the schema change
     */
    @SuppressWarnings("deprecation")
    static void applySchemaChangeToWriter(SinkWriter<?, ?, ?> writer, SchemaChangeEvent event)
            throws IOException {
        if (writer instanceof SupportSchemaEvolutionSinkWriter) {
            ((SupportSchemaEvolutionSinkWriter) writer).applySchemaChange(event);
            return;
        }
        if (overridesDeprecatedApplySchemaChange(writer)) {
            writer.applySchemaChange(event);
            return;
        }
        throw new UnsupportedOperationException(
                String.format(
                        "Sink writer %s received schema change event for table %s, but it does "
                                + "not implement SupportSchemaEvolutionSinkWriter or override "
                                + "SinkWriter.applySchemaChange(SchemaChangeEvent). "
                                + "Schema evolution requires a schema-evolution-capable sink.",
                        writer.getClass().getName(), event.tablePath()));
    }

    /**
     * Checks whether a legacy sink writer provides its own deprecated schema change implementation.
     *
     * @param writer sink writer to inspect
     * @return true when the writer overrides {@link
     *     SinkWriter#applySchemaChange(SchemaChangeEvent)}
     */
    static boolean overridesDeprecatedApplySchemaChange(SinkWriter<?, ?, ?> writer) {
        try {
            Method method =
                    writer.getClass().getMethod("applySchemaChange", SchemaChangeEvent.class);
            return method.getDeclaringClass() != SinkWriter.class;
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(
                    "SinkWriter.applySchemaChange(SchemaChangeEvent) is missing from the writer API",
                    e);
        }
    }
}

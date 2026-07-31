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

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Optional;

/**
 * Verifies the shared sink-side schema change support gate.
 *
 * <p>The gate must preserve explicit legacy implementations while preventing the default no-op
 * schema change method from silently accepting CDC schema changes.
 */
class SupportSchemaEvolutionSinkWriterTest {

    /**
     * Verifies the primary schema evolution extension point.
     *
     * @throws IOException if applying the schema change fails unexpectedly
     */
    @Test
    void shouldApplySchemaChangeWithSupportInterface() throws IOException {
        SupportInterfaceWriter writer = new SupportInterfaceWriter();
        SchemaChangeEvent event = schemaChangeEvent();

        SupportSchemaEvolutionSinkWriter.applySchemaChangeToWriter(writer, event);

        Assertions.assertSame(event, writer.appliedEvent);
    }

    /**
     * Verifies compatibility with writers that still override the deprecated hook.
     *
     * @throws IOException if applying the schema change fails unexpectedly
     */
    @Test
    void shouldKeepDeprecatedApplySchemaChangeOverrideCompatible() throws IOException {
        DeprecatedOverrideWriter writer = new DeprecatedOverrideWriter();
        SchemaChangeEvent event = schemaChangeEvent();

        SupportSchemaEvolutionSinkWriter.applySchemaChangeToWriter(writer, event);

        Assertions.assertSame(event, writer.appliedEvent);
    }

    /**
     * Verifies that inheriting the deprecated default no-op hook is not treated as schema evolution
     * support.
     */
    @Test
    void shouldFailFastWhenSinkWriterDoesNotSupportSchemaEvolution() {
        PlainWriter writer = new PlainWriter();
        SchemaChangeEvent event = schemaChangeEvent();

        UnsupportedOperationException exception =
                Assertions.assertThrows(
                        UnsupportedOperationException.class,
                        () ->
                                SupportSchemaEvolutionSinkWriter.applySchemaChangeToWriter(
                                        writer, event));

        Assertions.assertTrue(exception.getMessage().contains(PlainWriter.class.getName()));
        Assertions.assertTrue(exception.getMessage().contains("test_db.test_table"));
    }

    /**
     * Creates the schema change event used by all support-gate cases.
     *
     * @return a mocked schema change event with a stable table path
     */
    private static SchemaChangeEvent schemaChangeEvent() {
        SchemaChangeEvent event = Mockito.mock(SchemaChangeEvent.class);
        Mockito.when(event.tablePath()).thenReturn(TablePath.of("test_db", "test_table"));
        return event;
    }

    /**
     * Sink writer that inherits the deprecated default no-op schema change method.
     *
     * <p>This writer represents sinks that have no schema evolution support at all.
     */
    private static class PlainWriter implements SinkWriter<Object, Object, Object> {

        @Override
        public void write(Object element) {}

        @Override
        public Optional<Object> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() {}
    }

    /**
     * Sink writer that uses the current schema evolution extension point.
     *
     * <p>This is the preferred path for schema-evolution-capable sinks.
     */
    private static class SupportInterfaceWriter extends PlainWriter
            implements SupportSchemaEvolutionSinkWriter {

        private SchemaChangeEvent appliedEvent;

        @Override
        public void applySchemaChange(SchemaChangeEvent event) {
            this.appliedEvent = event;
        }
    }

    /**
     * Sink writer that keeps the legacy deprecated schema change extension point.
     *
     * <p>The shared support gate still accepts this path for backward compatibility.
     */
    private static class DeprecatedOverrideWriter extends PlainWriter {

        private SchemaChangeEvent appliedEvent;

        @Override
        @SuppressWarnings("deprecation")
        public void applySchemaChange(SchemaChangeEvent event) {
            this.appliedEvent = event;
        }
    }
}

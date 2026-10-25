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

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.error.RowErrorClassification;
import org.apache.seatunnel.api.common.error.RowErrorCollector;
import org.apache.seatunnel.api.common.error.SupportRowLevelErrorClassifier;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Verifies collector thresholds, serialization, lifecycle and safe writer-failure classification.
 *
 * <p>Real delegated rows and capabilities are reconciled so collection cannot hide fatal failures
 * or count rejected rows as successful writes.
 */
public class DirtyRecordCollectorTest {

    @Test
    void testConcurrentCollect() throws Exception {
        CountingDirtyRecordCollector collector = new CountingDirtyRecordCollector();
        int threadCount = 8;
        int recordsPerThread = 200;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        CountDownLatch countDownLatch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executorService.submit(
                    () -> {
                        try {
                            for (int j = 0; j < recordsPerThread; j++) {
                                collector.collect(0, new SeaTunnelRow(new Object[] {j}), null);
                            }
                        } finally {
                            countDownLatch.countDown();
                        }
                    });
        }

        Assertions.assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        executorService.shutdownNow();
        Assertions.assertEquals(threadCount * recordsPerThread, collector.getDirtyRecordCount());
    }

    @Test
    void testThresholdBoundary() {
        CountingDirtyRecordCollector collector = new CountingDirtyRecordCollector();
        collector.init(ConfigFactory.parseString("threshold=2\nfail_on_threshold=true"));

        Assertions.assertDoesNotThrow(
                () -> collector.collect(0, new SeaTunnelRow(new Object[] {1}), null));
        RuntimeException thresholdException =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> collector.collect(0, new SeaTunnelRow(new Object[] {2}), null));
        Assertions.assertTrue(thresholdException.getMessage().contains("threshold exceeded"));

        RuntimeException aboveThresholdException =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> collector.collect(0, new SeaTunnelRow(new Object[] {3}), null));
        Assertions.assertTrue(aboveThresholdException.getMessage().contains("threshold exceeded"));
    }

    @Test
    void testPayloadMaskingIsIndependentOfDefaultLocale() throws Exception {
        Locale previousLocale = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            LogDirtyRecordCollector collector = new LogDirtyRecordCollector();
            collector.init(ConfigFactory.parseString("log_payload=FULL\nmask_fields=[FIELD_0]"));
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {"private-value"});
            Method summarizeRecord =
                    LogDirtyRecordCollector.class.getDeclaredMethod(
                            "summarizeRecord", Object.class, CatalogTable.class);
            summarizeRecord.setAccessible(true);
            String output = (String) summarizeRecord.invoke(collector, row, null);
            Assertions.assertEquals("[field_0=***]", output);
            Assertions.assertFalse(output.contains("private-value"));
        } finally {
            Locale.setDefault(previousLocale);
        }
    }

    @Test
    void testNoOpReadResolveSingleton() throws Exception {
        Object restored = roundTrip(NoOpDirtyRecordCollector.INSTANCE);
        Assertions.assertSame(NoOpDirtyRecordCollector.INSTANCE, restored);
    }

    @Test
    void testDecoratorCollectsNonRowPayload() throws Exception {
        CountingDirtyRecordCollector collector = new CountingDirtyRecordCollector();
        DirtyDataAwareSinkWriter<String, Void, Void> writer =
                new DirtyDataAwareSinkWriter<>(new ClassifiedFailingWriter(), collector, 1);

        Assertions.assertDoesNotThrow(() -> writer.write("payload"));
        Assertions.assertEquals(1L, collector.getDirtyRecordCount());
    }

    @Test
    void testDecoratorPropagatesUnclassifiedSystemFailure() {
        CountingDirtyRecordCollector collector = new CountingDirtyRecordCollector();
        DirtyDataAwareSinkWriter<String, Void, Void> writer =
                new DirtyDataAwareSinkWriter<>(new FailingWriter(), collector, 1);
        IOException error =
                Assertions.assertThrows(IOException.class, () -> writer.write("payload"));
        Assertions.assertEquals("boom", error.getMessage());
        Assertions.assertEquals(0L, collector.getDirtyRecordCount());
    }

    @Test
    void testDecoratorPropagatesExplicitSystemClassification() {
        CountingDirtyRecordCollector collector = new CountingDirtyRecordCollector();
        ClassifiedFailingWriter delegate =
                new ClassifiedFailingWriter() {
                    @Override
                    public RowErrorClassification classifyRowError(Throwable error, String row) {
                        return RowErrorClassification.SYSTEM_ERROR;
                    }
                };
        DirtyDataAwareSinkWriter<String, Void, Void> writer =
                new DirtyDataAwareSinkWriter<>(delegate, collector, 1);
        IOException error =
                Assertions.assertThrows(IOException.class, () -> writer.write("payload"));
        Assertions.assertEquals("boom", error.getMessage());
        Assertions.assertEquals(0L, collector.getDirtyRecordCount());
    }

    @Test
    void testValidatingCollectorRequiresValidator() {
        Assertions.assertThrows(
                NullPointerException.class,
                () -> new ValidatingDirtyRecordCollector(NoOpDirtyRecordCollector.INSTANCE, null));
    }

    @Test
    void testCollectorServiceDiscovery() {
        DirtyRecordCollector collector =
                DirtyRecordCollectorFactory.createCollector(
                        ConfigFactory.parseString("type=counting"));
        Assertions.assertInstanceOf(CountingDirtyRecordCollector.class, collector);
    }

    @Test
    void testValidatorServiceDiscovery() {
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "db", "table"),
                        TableSchema.builder().build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "comment");
        DirtyDataValidator validator =
                DirtyDataValidatorFactory.createValidator(
                        "AlwaysDirtyDataValidator", ConfigFactory.empty(), catalogTable);
        Assertions.assertNotNull(validator);
        Assertions.assertTrue(
                validator.validate(new SeaTunnelRow(new Object[] {1}), catalogTable).isDirty());
    }

    @Test
    void testSinkWriterContextDefaultCollectorIsNoOp() {
        SinkWriter.Context context =
                new SinkWriter.Context() {
                    @Override
                    public int getIndexOfSubtask() {
                        return 0;
                    }

                    @Override
                    public MetricsContext getMetricsContext() {
                        return null;
                    }

                    @Override
                    public EventListener getEventListener() {
                        return null;
                    }
                };

        Assertions.assertSame(NoOpDirtyRecordCollector.INSTANCE, context.getDirtyRecordCollector());
    }

    @Test
    void testUnknownCollectorTypeFailsFast() {
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                DirtyRecordCollectorFactory.createCollector(
                                        ConfigFactory.parseString("type=missing")));
        Assertions.assertTrue(exception.getMessage().contains("Unknown dirty.collector type"));
    }

    @Test
    void testMissingValidatorTypeFailsFast() {
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                DirtyCollectorConfigProcessor.createValidator(
                                        ConfigFactory.parseString("dirty.validator={}"), null));
        Assertions.assertTrue(exception.getMessage().contains("missing required 'type'"));
    }

    @Test
    void testUnknownValidatorTypeFailsFast() {
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                DirtyCollectorConfigProcessor.createValidator(
                                        ConfigFactory.parseString(
                                                "dirty.validator { type = missing }"),
                                        null));
        Assertions.assertTrue(exception.getMessage().contains("Could not resolve dirty.validator"));
    }

    @Test
    void testDirtyDataAwareWriterClosesCollector() throws Exception {
        CloseTrackingCollector collector = new CloseTrackingCollector();
        DirtyDataAwareSinkWriter<String, Void, Void> writer =
                new DirtyDataAwareSinkWriter<>(new NoOpWriter(), collector, 1);

        writer.close();

        Assertions.assertTrue(collector.closed);
    }

    @Test
    void testDirtyDataAwareWriterClosesCollectorWhenDelegateCloseFails() {
        CloseTrackingCollector collector = new CloseTrackingCollector();
        DirtyDataAwareSinkWriter<String, Void, Void> writer =
                new DirtyDataAwareSinkWriter<>(new CloseFailingWriter(), collector, 1);

        IOException exception = Assertions.assertThrows(IOException.class, writer::close);

        Assertions.assertEquals("delegate-close", exception.getMessage());
        Assertions.assertTrue(collector.closed);
    }

    @Test
    void testNoFeatureFactoryPreservesOriginalWriterIdentity() {
        MultiTableSinkWriter raw = Mockito.mock(MultiTableSinkWriter.class);
        Assertions.assertSame(
                raw,
                DirtyDataAwareSinkWriter.wrap(
                        raw, NoOpDirtyRecordCollector.INSTANCE, 0, null, null));
    }

    @Test
    void testFactoryPreservesSchemaCapabilityAndPhysicalIdentity() throws Exception {
        TrackingSchemaWriter raw = new TrackingSchemaWriter();
        SinkWriter<String, Void, Void> wrapped =
                DirtyDataAwareSinkWriter.wrap(
                        raw, new CountingDirtyRecordCollector(), 0, null, null);
        Assertions.assertInstanceOf(SupportSchemaEvolutionSinkWriter.class, wrapped);
        SupportSchemaEvolutionSinkWriter schemaWriter = (SupportSchemaEvolutionSinkWriter) wrapped;
        SchemaChangeEvent event = Mockito.mock(SchemaChangeEvent.class);
        schemaWriter.applySchemaChange(event);
        Assertions.assertSame(event, raw.lastSchemaChange);
        Assertions.assertEquals(
                Optional.of("physical_table"), schemaWriter.getPhysicalSinkTableIdentifier());
    }

    @Test
    void testFactoryDoesNotInventSchemaCapability() {
        SinkWriter<String, Void, Void> wrapped =
                DirtyDataAwareSinkWriter.wrap(
                        new NoOpWriter(), new CountingDirtyRecordCollector(), 0, null, null);
        Assertions.assertFalse(wrapped instanceof SupportSchemaEvolutionSinkWriter);
    }

    @Test
    void testFactoryRejectsActiveDirtyHandlingForMultiTableWriter() {
        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                DirtyDataAwareSinkWriter.wrap(
                                        Mockito.mock(MultiTableSinkWriter.class),
                                        new CountingDirtyRecordCollector(),
                                        0,
                                        null,
                                        null));
        Assertions.assertTrue(error.getMessage().contains("synchronous single-table"));
    }

    @Test
    void testFactoryRejectsDeferredTerminalOutcomes() {
        SinkWriter.Context context = Mockito.mock(SinkWriter.Context.class);
        Mockito.when(context.getRowErrorCollector()).thenReturn(Optional.empty());
        Mockito.when(context.isDeferredTerminalWriteOutcomesEnabled()).thenReturn(true);
        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                DirtyDataAwareSinkWriter.wrap(
                                        new NoOpWriter(),
                                        new CountingDirtyRecordCollector(),
                                        0,
                                        null,
                                        context));
        Assertions.assertTrue(error.getMessage().contains("deferred terminal outcomes"));
    }

    @Test
    void testFactoryRejectsRowErrorRoutingAndClosesAllocatedResources() throws Exception {
        SinkWriter.Context context = Mockito.mock(SinkWriter.Context.class);
        Mockito.when(context.getRowErrorCollector())
                .thenReturn(Optional.of(Mockito.mock(RowErrorCollector.class)));
        SinkWriter<String, Void, Void> raw = Mockito.mock(SinkWriter.class);
        CloseTrackingCollector collector = new CloseTrackingCollector();
        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> DirtyDataAwareSinkWriter.wrap(raw, collector, 0, null, context));
        Assertions.assertTrue(error.getMessage().contains("row-error routing"));
        Mockito.verify(raw).close();
        Mockito.verify(raw, Mockito.never()).write(Mockito.anyString());
        Assertions.assertTrue(collector.closed);
    }

    @Test
    void testMixedValidationReportsExactDelegateRowsAndCollection() throws Exception {
        TrackingRowWriter raw = new TrackingRowWriter();
        CountingDirtyRecordCollector collector = new CountingDirtyRecordCollector();
        DirtyDataValidator validator =
                (row, table) ->
                        ((Integer) row.getField(0)) < 0
                                ? DirtyDataValidator.ValidationResult.dirty("negative id")
                                : DirtyDataValidator.ValidationResult.clean();
        DirtyDataAwareSinkWriter<SeaTunnelRow, Void, Void> writer =
                new DirtyDataAwareSinkWriter<>(
                        raw, new ValidatingDirtyRecordCollector(collector, validator), 0);
        SeaTunnelRow rejected = new SeaTunnelRow(new Object[] {-1});
        SeaTunnelRow accepted = new SeaTunnelRow(new Object[] {1});
        Assertions.assertFalse(writer.writeAndReport(rejected));
        Assertions.assertTrue(writer.writeAndReport(accepted));
        Assertions.assertEquals(Collections.singletonList(accepted), raw.rows);
        Assertions.assertEquals(1L, collector.getDirtyRecordCount());
    }

    @Test
    void testRuntimeCloseFailureRetainsCollectorFailureAsSuppressed() {
        CloseTrackingCollector collector =
                new CloseTrackingCollector() {
                    @Override
                    public void close() {
                        super.close();
                        throw new IllegalStateException("collector-close");
                    }
                };
        NoOpWriter raw =
                new NoOpWriter() {
                    @Override
                    public void close() {
                        throw new IllegalStateException("delegate-runtime");
                    }
                };
        DirtyDataAwareSinkWriter<String, Void, Void> writer =
                new DirtyDataAwareSinkWriter<>(raw, collector, 0);
        IllegalStateException error =
                Assertions.assertThrows(IllegalStateException.class, writer::close);
        Assertions.assertEquals("delegate-runtime", error.getMessage());
        Assertions.assertEquals(1, error.getSuppressed().length);
        Assertions.assertEquals("collector-close", error.getSuppressed()[0].getMessage());
        Assertions.assertTrue(collector.closed);
    }

    private Object roundTrip(Object value) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            outputStream.writeObject(value);
        }
        try (ObjectInputStream inputStream =
                new ObjectInputStream(
                        new ByteArrayInputStream(byteArrayOutputStream.toByteArray()))) {
            return inputStream.readObject();
        }
    }

    /**
     * Records schema callbacks without introducing any schema capability on the base writer.
     *
     * <p>The explicit marker and physical identifier are the contracts the wrapper must preserve.
     */
    private static final class TrackingSchemaWriter extends NoOpWriter
            implements SupportSchemaEvolutionSinkWriter {
        private SchemaChangeEvent lastSchemaChange;

        @Override
        public void applySchemaChange(SchemaChangeEvent event) {
            lastSchemaChange = event;
        }

        @Override
        public Optional<String> getPhysicalSinkTableIdentifier() {
            return Optional.of("physical_table");
        }
    }

    /**
     * Retains actual delegated rows for validation/output reconciliation.
     *
     * <p>Rows rejected before write must never enter this list, independent of collector counters.
     */
    private static final class TrackingRowWriter implements SinkWriter<SeaTunnelRow, Void, Void> {
        private final List<SeaTunnelRow> rows = new ArrayList<>();

        @Override
        public void write(SeaTunnelRow row) {
            rows.add(row);
        }

        @Override
        public Optional<Void> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() {}
    }

    /**
     * Produces an unclassified delegate failure for fail-closed write tests.
     *
     * <p>A collector must never convert this failure into a successful sink write.
     */
    private static class FailingWriter implements SinkWriter<String, Void, Void> {

        @Override
        public void write(String element) throws IOException {
            throw new IOException("boom");
        }

        @Override
        public Optional<Void> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() throws IOException {}
    }

    /**
     * Reports a deterministic row error for the collection branch.
     *
     * <p>The unclassified base writer remains a separate fixture so system-failure propagation
     * cannot pass merely because every test exception was marked recoverable.
     */
    private static class ClassifiedFailingWriter extends FailingWriter
            implements SupportRowLevelErrorClassifier<String> {
        @Override
        public RowErrorClassification classifyRowError(Throwable error, String row) {
            return RowErrorClassification.ROW_ERROR;
        }
    }

    /**
     * Supplies an ordinary writer without schema or row-classification capabilities.
     *
     * <p>Wrapper tests must preserve this absence instead of inventing optional contracts.
     */
    private static class NoOpWriter implements SinkWriter<String, Void, Void> {

        @Override
        public void write(String element) {}

        @Override
        public Optional<Void> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() throws IOException {}
    }

    /**
     * Fails writer closure before collector cleanup is attempted.
     *
     * <p>The delegate failure must remain primary when collector closure also fails.
     */
    private static class CloseFailingWriter extends NoOpWriter {

        @Override
        public void close() throws IOException {
            throw new IOException("delegate-close");
        }
    }

    /**
     * Records collector cleanup independently of writer close success.
     *
     * <p>Close-order tests use this flag to prove resources are released on exceptional paths.
     */
    private static class CloseTrackingCollector implements DirtyRecordCollector {
        private boolean closed;

        @Override
        public void collect(
                int subTaskIndex,
                Object dirtyRecord,
                Throwable exception,
                String errorMessage,
                CatalogTable catalogTable) {}

        @Override
        public void close() {
            closed = true;
        }
    }
}

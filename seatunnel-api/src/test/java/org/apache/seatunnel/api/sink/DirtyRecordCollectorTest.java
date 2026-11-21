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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies collector thresholds, serialization, lifecycle and unchanged writer-failure propagation.
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
        Logger logger = (Logger) LogManager.getLogger(LogDirtyRecordCollector.class);
        Level previousLevel = logger.getLevel();
        List<String> messages = new CopyOnWriteArrayList<>();
        AbstractAppender appender =
                new AbstractAppender(
                        "dirty-locale-test",
                        null,
                        PatternLayout.createDefaultLayout(),
                        true,
                        Property.EMPTY_ARRAY) {
                    @Override
                    public void append(LogEvent event) {
                        messages.add(event.getMessage().getFormattedMessage());
                    }
                };
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.ERROR);
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            LogDirtyRecordCollector collector = new LogDirtyRecordCollector();
            collector.init(ConfigFactory.parseString("log_payload=FULL\nmask_fields=[FIELD_0]"));
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {"private-value"});
            collector.collectFromUserRule(0, row, "locale-mask", null);
            Awaitility.await()
                    .atMost(Duration.ofSeconds(5))
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        Collections.singletonList(
                                                "Dirty record collected (user rule) - SubTask: 0, Table: unknown, Count: 1, Record: [field_0=***], Reason: locale-mask"),
                                        messages);
                                Assertions.assertFalse(messages.get(0).contains("private-value"));
                            });
            Assertions.assertEquals(1L, collector.getDirtyRecordCount());
        } finally {
            logger.removeAppender(appender);
            appender.stop();
            logger.setLevel(previousLevel);
            Locale.setDefault(previousLocale);
        }
    }

    @Test
    void testDefaultLogSamplingRetainsEveryCountAndOneCloseSummary() throws Exception {
        List<String> messages = new CopyOnWriteArrayList<>();
        withLogMessages(
                messages,
                () -> {
                    LogDirtyRecordCollector collector = new LogDirtyRecordCollector();
                    collector.init(ConfigFactory.empty());
                    for (int i = 1; i <= 10000; i++) {
                        collector.collectFromUserRule(
                                0, new SeaTunnelRow(new Object[] {i}), "rule", null);
                    }
                    Assertions.assertEquals(10000L, collector.getDirtyRecordCount());
                    collector.close();
                    collector.close();
                    List<String> expected = new ArrayList<>();
                    for (int count :
                            new int[] {
                                1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                                1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000
                            }) {
                        expected.add(
                                "Dirty record collected (user rule) - SubTask: 0, Table: unknown, Count: "
                                        + count
                                        + ", Record: fields=1, Reason: rule");
                    }
                    expected.add(
                            "Dirty record collection summary - Observed: 10000, Logged: 20, Suppressed: 9980");
                    Awaitility.await()
                            .atMost(Duration.ofSeconds(5))
                            .untilAsserted(() -> Assertions.assertEquals(expected, messages));
                });
    }

    @Test
    void testDisabledLogLevelDoesNotRenderPayloadOrSkipFatalThreshold() throws Exception {
        List<String> messages = new CopyOnWriteArrayList<>();
        AtomicInteger renders = new AtomicInteger();
        SeaTunnelRow row = unrenderableRow(renders);
        AtomicInteger exceptionMessages = new AtomicInteger();
        RuntimeException exception =
                new RuntimeException() {
                    @Override
                    public String getMessage() {
                        exceptionMessages.incrementAndGet();
                        throw new AssertionError(
                                "Disabled exception details must not be formatted");
                    }
                };
        withLogMessages(
                messages,
                () -> {
                    LogDirtyRecordCollector collector = new LogDirtyRecordCollector();
                    collector.init(
                            ConfigFactory.parseString(
                                    "log_level=DEBUG\nlog_payload=full\nthreshold=2\nfail_on_threshold=true"));
                    collector.collect(0, row, exception, "rule", null);
                    RuntimeException failure =
                            Assertions.assertThrows(
                                    RuntimeException.class,
                                    () -> collector.collectFromUserRule(0, row, "rule", null));
                    Assertions.assertEquals(RuntimeException.class, failure.getCause().getClass());
                    Assertions.assertEquals(
                            "Dirty record threshold exceeded: 2 >= 2",
                            failure.getCause().getMessage());
                    Assertions.assertEquals(2L, collector.getDirtyRecordCount());
                    Assertions.assertEquals(0, renders.get());
                    Assertions.assertEquals(0, exceptionMessages.get());
                    collector.close();
                    Awaitility.await()
                            .atMost(Duration.ofSeconds(5))
                            .untilAsserted(
                                    () ->
                                            Assertions.assertEquals(
                                                    Collections.singletonList(
                                                            "Dirty record threshold exceeded: 2 >= 2 - Task will be failed!"),
                                                    messages));
                });
    }

    @Test
    void testUnsampledPayloadIsLazyAndStillCounts() throws Exception {
        List<String> messages = new CopyOnWriteArrayList<>();
        AtomicInteger renders = new AtomicInteger();
        withLogMessages(
                messages,
                () -> {
                    LogDirtyRecordCollector collector = new LogDirtyRecordCollector();
                    collector.init(
                            ConfigFactory.parseString(
                                    "log_payload=full\nlog_first_n_records=1\nlog_every_n_records=3"));
                    collector.collectFromUserRule(
                            0, new SeaTunnelRow(new Object[] {"first"}), "rule", null);
                    collector.collect(0, unrenderableRow(renders), null, "rule", null);
                    collector.collectFromUserRule(
                            0, new SeaTunnelRow(new Object[] {"third"}), "rule", null);
                    collector.close();
                    Assertions.assertEquals(3L, collector.getDirtyRecordCount());
                    Assertions.assertEquals(0, renders.get());
                    Awaitility.await()
                            .atMost(Duration.ofSeconds(5))
                            .untilAsserted(
                                    () ->
                                            Assertions.assertEquals(
                                                    Arrays.asList(
                                                            "Dirty record collected (user rule) - SubTask: 0, Table: unknown, Count: 1, Record: [field_0=first], Reason: rule",
                                                            "Dirty record collected (user rule) - SubTask: 0, Table: unknown, Count: 3, Record: [field_0=third], Reason: rule",
                                                            "Dirty record collection summary - Observed: 3, Logged: 2, Suppressed: 1"),
                                                    messages));
                });
    }

    @Test
    void testInvalidLogSamplingIntervalsFailDuringInitialization() {
        for (String config :
                new String[] {
                    "log_first_n_records=-1", "log_every_n_records=0", "log_every_n_records=-1"
                }) {
            IllegalArgumentException failure =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () ->
                                    new LogDirtyRecordCollector()
                                            .init(ConfigFactory.parseString(config)));
            Assertions.assertTrue(
                    failure.getMessage().contains(config.substring(0, config.indexOf('='))));
        }
    }

    @Test
    void testNoOpReadResolveSingleton() throws Exception {
        Object restored = roundTrip(NoOpDirtyRecordCollector.INSTANCE);
        Assertions.assertSame(NoOpDirtyRecordCollector.INSTANCE, restored);
    }

    @Test
    void testDecoratorPropagatesEvenClassifiedRowFailuresUnchanged() throws Exception {
        CountingDirtyRecordCollector collector = new CountingDirtyRecordCollector();
        ClassifiedFailingWriter delegate = Mockito.spy(new ClassifiedFailingWriter());
        IOException failure = new IOException("buffered earlier row failed");
        Mockito.doThrow(failure).when(delegate).write("healthy current row");
        DirtyDataAwareSinkWriter<String, Void, Void> writer =
                new DirtyDataAwareSinkWriter<>(delegate, collector, 1);

        Assertions.assertSame(
                failure,
                Assertions.assertThrows(
                        IOException.class, () -> writer.write("healthy current row")));
        Assertions.assertEquals(0L, collector.getDirtyRecordCount());
        Mockito.verify(delegate, Mockito.never()).classifyRowError(Mockito.any(), Mockito.any());
    }

    @Test
    void testCountingCollectorNeverFormatsPayloadAndWarnsOnce() throws Exception {
        Logger logger = (Logger) LogManager.getLogger(CountingDirtyRecordCollector.class);
        Level previousLevel = logger.getLevel();
        List<String> messages = new CopyOnWriteArrayList<>();
        List<Level> levels = new CopyOnWriteArrayList<>();
        AbstractAppender appender =
                new AbstractAppender(
                        "counting-privacy-test",
                        null,
                        PatternLayout.createDefaultLayout(),
                        true,
                        Property.EMPTY_ARRAY) {
                    @Override
                    public void append(LogEvent event) {
                        messages.add(event.getMessage().getFormattedMessage());
                        levels.add(event.getLevel());
                    }
                };
        AtomicInteger formatted = new AtomicInteger();
        Object privatePayload =
                new Object() {
                    @Override
                    public String toString() {
                        formatted.incrementAndGet();
                        throw new AssertionError("private-payload-sentinel");
                    }
                };
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.DEBUG);
        try {
            CountingDirtyRecordCollector collector = new CountingDirtyRecordCollector();
            collector.init(ConfigFactory.parseString("threshold=2\nfail_on_threshold=false"));
            for (int i = 0; i < 4; i++) {
                collector.collect(0, privatePayload, null, "private-error-sentinel", null);
            }
            collector.checkThreshold();
            collector.close();
            collector.close();
            Awaitility.await()
                    .atMost(Duration.ofSeconds(5))
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        1L, levels.stream().filter(Level.WARN::equals).count());
                                Assertions.assertEquals(
                                        1L,
                                        messages.stream()
                                                .filter(
                                                        "[CountingCollector] total dirty records=4"
                                                                ::equals)
                                                .count());
                            });
            Assertions.assertEquals(4L, collector.getDirtyRecordCount());
            Assertions.assertEquals(0, formatted.get());
            Assertions.assertTrue(messages.stream().noneMatch(value -> value.contains("private-")));
            Assertions.assertFalse(levels.contains(Level.ERROR));
        } finally {
            logger.removeAppender(appender);
            appender.stop();
            logger.setLevel(previousLevel);
        }
    }

    @Test
    void testDecoratorPreservesRuntimeWriteFailure() throws Exception {
        CountingDirtyRecordCollector collector = new CountingDirtyRecordCollector();
        NoOpWriter delegate = Mockito.mock(NoOpWriter.class);
        IllegalStateException failure = new IllegalStateException("latched failure");
        Mockito.doThrow(failure).when(delegate).write("healthy row");
        DirtyDataAwareSinkWriter<String, Void, Void> writer =
                new DirtyDataAwareSinkWriter<>(delegate, collector, 0);
        Assertions.assertSame(
                failure,
                Assertions.assertThrows(
                        IllegalStateException.class, () -> writer.write("healthy row")));
        Assertions.assertEquals(0L, collector.getDirtyRecordCount());
    }

    @Test
    void testLogCollectorNonfatalThresholdWarnsOnce() throws Exception {
        Logger logger = (Logger) LogManager.getLogger(LogDirtyRecordCollector.class);
        Level previousLevel = logger.getLevel();
        List<String> messages = new CopyOnWriteArrayList<>();
        AbstractAppender appender =
                new AbstractAppender(
                        "log-threshold-test",
                        null,
                        PatternLayout.createDefaultLayout(),
                        true,
                        Property.EMPTY_ARRAY) {
                    @Override
                    public void append(LogEvent event) {
                        if (event.getLevel() == Level.WARN) {
                            messages.add(event.getMessage().getFormattedMessage());
                        }
                    }
                };
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.WARN);
        try {
            LogDirtyRecordCollector collector = new LogDirtyRecordCollector();
            collector.init(
                    ConfigFactory.parseString(
                            "threshold=2\nfail_on_threshold=false\nlog_level=DEBUG"));
            for (int i = 0; i < 4; i++) {
                collector.collectFromUserRule(0, new SeaTunnelRow(new Object[] {i}), "rule", null);
            }
            collector.checkThreshold();
            Awaitility.await()
                    .atMost(Duration.ofSeconds(5))
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            Collections.singletonList(
                                                    "Dirty record threshold exceeded: 2 >= 2 - Threshold reached but task continues"),
                                            messages));
            Assertions.assertEquals(4L, collector.getDirtyRecordCount());
        } finally {
            logger.removeAppender(appender);
            appender.stop();
            logger.setLevel(previousLevel);
        }
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

    private static SeaTunnelRow unrenderableRow(AtomicInteger renders) {
        return new SeaTunnelRow(
                new Object[] {
                    new Object() {
                        @Override
                        public String toString() {
                            renders.incrementAndGet();
                            throw new AssertionError("Suppressed payload must not be rendered");
                        }
                    }
                });
    }

    /**
     * Captures the collector's actual log messages while keeping the logger configuration scoped.
     * Restoring the prior level and removing the appender also covers failed assertions.
     */
    private static void withLogMessages(List<String> messages, Executable action) throws Exception {
        Logger logger = (Logger) LogManager.getLogger(LogDirtyRecordCollector.class);
        Level previousLevel = logger.getLevel();
        AbstractAppender appender =
                new AbstractAppender(
                        "log-sampling-test",
                        null,
                        PatternLayout.createDefaultLayout(),
                        true,
                        Property.EMPTY_ARRAY) {
                    @Override
                    public void append(LogEvent event) {
                        messages.add(event.getMessage().getFormattedMessage());
                    }
                };
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.ERROR);
        try {
            action.execute();
        } catch (Exception | Error failure) {
            throw failure;
        } catch (Throwable failure) {
            throw new AssertionError(failure);
        } finally {
            logger.removeAppender(appender);
            appender.stop();
            logger.setLevel(previousLevel);
        }
    }
}

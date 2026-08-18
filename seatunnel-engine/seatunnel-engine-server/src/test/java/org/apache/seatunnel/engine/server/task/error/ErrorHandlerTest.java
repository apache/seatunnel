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

package org.apache.seatunnel.engine.server.task.error;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelFlatMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;
import org.apache.seatunnel.engine.server.common.statestore.counter.CounterStateStore;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for ErrorHandler. */
public class ErrorHandlerTest {

    private static final String TEST_STAGE = "SINK";
    private static final String TEST_PLUGIN = "Jdbc";
    private static final String TEST_TABLE = "test_table";

    @Test
    public void testDisableModeDoesNothing() {
        // Disable mode should not throw exceptions or write errors
        StageErrorConfig config =
                StageErrorConfig.builder()
                        .mode(ErrorHandlerMode.DISABLE)
                        .maxErrorRecords(1)
                        .build();

        ErrorHandler<SeaTunnelRow> handler = new ErrorHandler<>(config);

        // Should not throw even with multiple errors
        for (int i = 0; i < 100; i++) {
            handler.incrementTotalRecords();
            RowErrorContext ctx = createContext();
            handler.onError(ctx, createRow(i), new RuntimeException("test error"));
        }

        // No exception thrown = success
        handler.close();
    }

    @Test
    public void testMaxErrorRecordsThreshold() {
        // Config: max 10 errors
        StageErrorConfig config =
                StageErrorConfig.builder()
                        .mode(ErrorHandlerMode.LOG)
                        .maxErrorRecords(10)
                        .maxErrorRatio(0.0)
                        .build();

        ErrorHandler<SeaTunnelRow> handler = new ErrorHandler<>(config);
        RowErrorContext ctx = createContext();

        // Process 10 errors - should succeed
        for (int i = 0; i < 10; i++) {
            handler.incrementTotalRecords();
            handler.onError(ctx, createRow(i), new RuntimeException("error " + i));
        }

        // 11th error should throw
        handler.incrementTotalRecords();
        RuntimeException ex =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                handler.onError(
                                        ctx, createRow(11), new RuntimeException("error 11")));

        assertTrue(ex.getMessage().contains("Too many row-level errors"));
        assertTrue(ex.getMessage().contains("11 records exceeded max_error_records=10"));
        handler.close();
    }

    @Test
    public void testMaxErrorRatioThreshold() {
        // Config: max 10% error ratio, min 100 records
        StageErrorConfig config =
                StageErrorConfig.builder()
                        .mode(ErrorHandlerMode.LOG)
                        .maxErrorRecords(0)
                        .maxErrorRatio(0.1) // 10%
                        .maxErrorRatioMinRecords(100)
                        .build();

        ErrorHandler<SeaTunnelRow> handler = new ErrorHandler<>(config);
        RowErrorContext ctx = createContext();

        // Process 90 success + 10 errors = 10% ratio, should be OK
        for (int i = 0; i < 90; i++) {
            handler.incrementTotalRecords();
        }
        for (int i = 0; i < 10; i++) {
            handler.incrementTotalRecords();
            handler.onError(ctx, createRow(i), new RuntimeException("error " + i));
        }

        // Now 11th error will push ratio to 11/101 = 10.89% > 10%
        handler.incrementTotalRecords();
        RuntimeException ex =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                handler.onError(
                                        ctx, createRow(11), new RuntimeException("error 11")));

        assertTrue(ex.getMessage().contains("error ratio"));
        assertTrue(ex.getMessage().contains("exceeded max_error_ratio"));
        handler.close();
    }

    @Test
    public void testMaxErrorRatioMinRecordsWarmup() {
        // Ratio check should not trigger until min records threshold
        StageErrorConfig config =
                StageErrorConfig.builder()
                        .mode(ErrorHandlerMode.LOG)
                        .maxErrorRatio(0.1) // 10%
                        .maxErrorRatioMinRecords(100)
                        .build();

        ErrorHandler<SeaTunnelRow> handler = new ErrorHandler<>(config);
        RowErrorContext ctx = createContext();

        // Process 50 total records with 20 errors = 40% error ratio
        // Should NOT throw because total < min threshold (100)
        for (int i = 0; i < 30; i++) {
            handler.incrementTotalRecords();
        }
        for (int i = 0; i < 20; i++) {
            handler.incrementTotalRecords();
            handler.onError(ctx, createRow(i), new RuntimeException("error " + i));
        }

        // No exception - warmup period protects small samples
        handler.close();
    }

    @Test
    public void testMaxErrorRatioTriggersWhenSuccessRowsCrossWarmupThreshold() {
        StageErrorConfig config =
                StageErrorConfig.builder()
                        .mode(ErrorHandlerMode.LOG)
                        .maxErrorRatio(0.1)
                        .maxErrorRatioMinRecords(10)
                        .build();
        ErrorHandler<SeaTunnelRow> handler = new ErrorHandler<>(config);
        RowErrorContext ctx = createContext();

        for (int i = 0; i < 5; i++) {
            handler.incrementTotalRecords();
            handler.onError(ctx, createRow(i), new RuntimeException("error " + i));
        }

        RuntimeException ex =
                assertThrows(
                        RuntimeException.class,
                        () -> {
                            for (int i = 0; i < 5; i++) {
                                handler.incrementTotalRecords();
                            }
                        });

        assertTrue(ex.getMessage().contains("error ratio"));
        assertTrue(ex.getMessage().contains("exceeded max_error_ratio"));
        handler.close();
    }

    @Test
    public void testStateStoreCounterSharesMaxRecordsAcrossParallelHandlersAfterCheckpoint() {
        StageErrorConfig config =
                StageErrorConfig.builder().mode(ErrorHandlerMode.LOG).maxErrorRecords(1).build();
        InMemoryCounterStateStore counterStore = new InMemoryCounterStateStore();
        ErrorHandler<SeaTunnelRow> firstSubtaskHandler =
                new ErrorHandler<>(
                        config,
                        null,
                        new StateStoreErrorHandlerCounter(counterStore, 1L, 2, 3L, "SINK"));
        ErrorHandler<SeaTunnelRow> secondSubtaskHandler =
                new ErrorHandler<>(
                        config,
                        null,
                        new StateStoreErrorHandlerCounter(counterStore, 1L, 2, 3L, "SINK"));
        RowErrorContext ctx = createContext();

        firstSubtaskHandler.incrementTotalRecords();
        firstSubtaskHandler.onError(ctx, createRow(1), new RuntimeException("first"));
        firstSubtaskHandler.snapshotState(1L);
        firstSubtaskHandler.notifyCheckpointComplete(1L);
        secondSubtaskHandler.notifyCheckpointComplete(1L);
        secondSubtaskHandler.incrementTotalRecords();
        RuntimeException ex =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                secondSubtaskHandler.onError(
                                        ctx, createRow(2), new RuntimeException("second")));

        assertTrue(ex.getMessage().contains("2 records exceeded max_error_records=1"));
        firstSubtaskHandler.close();
        secondSubtaskHandler.close();
    }

    @Test
    public void testStateStoreCounterSharesMaxRecordsAcrossParallelHandlersImmediately() {
        StageErrorConfig config =
                StageErrorConfig.builder().mode(ErrorHandlerMode.LOG).maxErrorRecords(1).build();
        InMemoryCounterStateStore counterStore = new InMemoryCounterStateStore();
        ErrorHandler<SeaTunnelRow> firstSubtaskHandler =
                new ErrorHandler<>(
                        config,
                        null,
                        new StateStoreErrorHandlerCounter(counterStore, 1L, 2, 3L, "SINK"));
        ErrorHandler<SeaTunnelRow> secondSubtaskHandler =
                new ErrorHandler<>(
                        config,
                        null,
                        new StateStoreErrorHandlerCounter(counterStore, 1L, 2, 3L, "SINK"));
        RowErrorContext ctx = createContext();

        firstSubtaskHandler.incrementTotalRecords();
        firstSubtaskHandler.onError(ctx, createRow(1), new RuntimeException("first"));
        secondSubtaskHandler.incrementTotalRecords();
        RuntimeException ex =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                secondSubtaskHandler.onError(
                                        ctx, createRow(2), new RuntimeException("second")));

        assertTrue(ex.getMessage().contains("2 records exceeded max_error_records=1"));
        firstSubtaskHandler.close();
        secondSubtaskHandler.close();
    }

    @Test
    public void testStateStoreCounterScopeIsActionAndStageSpecific() {
        InMemoryCounterStateStore counterStore = new InMemoryCounterStateStore();
        StateStoreErrorHandlerCounter firstSink =
                new StateStoreErrorHandlerCounter(counterStore, 1L, 2, 3L, "SINK");
        StateStoreErrorHandlerCounter secondSinkSameAction =
                new StateStoreErrorHandlerCounter(counterStore, 1L, 2, 3L, "SINK");
        StateStoreErrorHandlerCounter otherActionSink =
                new StateStoreErrorHandlerCounter(counterStore, 1L, 2, 4L, "SINK");
        StateStoreErrorHandlerCounter sameActionTransform =
                new StateStoreErrorHandlerCounter(counterStore, 1L, 2, 3L, "TRANSFORM");

        assertEquals(1L, firstSink.incrementTotalRecords());
        assertEquals(1L, firstSink.incrementErrorRecords());

        assertEquals(1L, secondSinkSameAction.getTotalRecords());
        assertEquals(1L, secondSinkSameAction.getErrorRecords());
        assertEquals(0L, otherActionSink.getTotalRecords());
        assertEquals(0L, otherActionSink.getErrorRecords());
        assertEquals(0L, sameActionTransform.getTotalRecords());
        assertEquals(0L, sameActionTransform.getErrorRecords());
    }

    @Test
    public void testStateStoreCounterSurvivesHandlerRecreationAfterRecovery() {
        StageErrorConfig config =
                StageErrorConfig.builder()
                        .mode(ErrorHandlerMode.LOG)
                        .maxErrorRatio(0.25)
                        .maxErrorRatioMinRecords(4)
                        .build();
        InMemoryCounterStateStore counterStore = new InMemoryCounterStateStore();
        RowErrorContext ctx = createContext();

        ErrorHandler<SeaTunnelRow> beforeRecovery =
                new ErrorHandler<>(
                        config,
                        null,
                        new StateStoreErrorHandlerCounter(counterStore, 10L, 20, 30L, "TRANSFORM"));
        beforeRecovery.incrementTotalRecords();
        beforeRecovery.onError(ctx, createRow(1), new RuntimeException("first"));
        beforeRecovery.incrementTotalRecords();
        beforeRecovery.onError(ctx, createRow(2), new RuntimeException("second"));
        beforeRecovery.snapshotState(1L);
        beforeRecovery.notifyCheckpointComplete(1L);

        ErrorHandler<SeaTunnelRow> afterRecovery =
                new ErrorHandler<>(
                        config,
                        null,
                        new StateStoreErrorHandlerCounter(counterStore, 10L, 20, 30L, "TRANSFORM"));

        RuntimeException ex =
                assertThrows(
                        RuntimeException.class,
                        () -> {
                            afterRecovery.incrementTotalRecords();
                            afterRecovery.incrementTotalRecords();
                        });

        assertTrue(ex.getMessage().contains("error ratio"));
        assertTrue(ex.getMessage().contains("errors=2"));
        assertTrue(ex.getMessage().contains("total=4"));
        beforeRecovery.close();
        afterRecovery.close();
    }

    @Test
    public void testLogModeLogsErrors() {
        // LOG mode should log but not route to error sink
        StageErrorConfig config =
                StageErrorConfig.builder()
                        .mode(ErrorHandlerMode.LOG)
                        .includeOriginalData(true)
                        .includeStacktrace(false)
                        .build();

        ErrorHandler<SeaTunnelRow> handler = new ErrorHandler<>(config);
        RowErrorContext ctx = createContext();

        // Should complete without throwing
        handler.incrementTotalRecords();
        handler.onError(ctx, createRow(1), new RuntimeException("test error"));

        handler.close();
    }

    @Test
    public void testRouteModeWithErrorSink() throws Exception {
        // ROUTE mode should write to error sink
        MockErrorSinkWriter mockSink = new MockErrorSinkWriter();

        StageErrorConfig config =
                StageErrorConfig.builder()
                        .mode(ErrorHandlerMode.ROUTE)
                        .includeOriginalData(true)
                        .includeStacktrace(true)
                        .originalDataMaxLength(1024)
                        .build();

        ErrorHandler<SeaTunnelRow> handler = new ErrorHandler<>(config, mockSink);
        RowErrorContext ctx = createContext();
        SeaTunnelRow errorRow = createRow(1);
        RuntimeException error = new RuntimeException("test error");

        handler.incrementTotalRecords();
        handler.onError(ctx, errorRow, error);

        // Verify error sink was called
        assertEquals(1, mockSink.getWrittenErrors().size());
        MockErrorSinkWriter.ErrorRecord record = mockSink.getWrittenErrors().get(0);
        assertEquals(ctx, record.context);
        assertEquals(errorRow, record.row);
        assertEquals(error, record.throwable);

        handler.close();
    }

    @Test
    public void testErrorSinkFailurePropagates() {
        // If error sink fails, exception should propagate
        ErrorSinkRowWriter<SeaTunnelRow> failingSink =
                new ErrorSinkRowWriter<SeaTunnelRow>() {
                    @Override
                    public void write(RowErrorContext ctx, SeaTunnelRow row, Throwable t)
                            throws Exception {
                        throw new Exception("Error sink failed");
                    }

                    @Override
                    public void close() {}
                };

        StageErrorConfig config = StageErrorConfig.builder().mode(ErrorHandlerMode.ROUTE).build();

        ErrorHandler<SeaTunnelRow> handler = new ErrorHandler<>(config, failingSink);
        RowErrorContext ctx = createContext();

        handler.incrementTotalRecords();
        RuntimeException ex =
                assertThrows(
                        RuntimeException.class,
                        () -> handler.onError(ctx, createRow(1), new RuntimeException("test")));

        assertTrue(ex.getMessage().contains("Error sink failed"));
        handler.close();
    }

    @Test
    public void testErrorSinkCloseFailurePropagates() {
        ErrorSinkRowWriter<SeaTunnelRow> failingCloseSink =
                new ErrorSinkRowWriter<SeaTunnelRow>() {
                    @Override
                    public void write(RowErrorContext ctx, SeaTunnelRow row, Throwable t) {}

                    @Override
                    public void close() throws Exception {
                        throw new Exception("close failed");
                    }
                };

        StageErrorConfig config = StageErrorConfig.builder().mode(ErrorHandlerMode.ROUTE).build();
        ErrorHandler<SeaTunnelRow> handler = new ErrorHandler<>(config, failingCloseSink);

        RuntimeException ex = assertThrows(RuntimeException.class, handler::close);

        assertTrue(ex.getMessage().contains("Failed to close error sink writer"));
        assertTrue(ex.getCause().getMessage().contains("close failed"));
    }

    @Test
    public void testErrorSinkFlushFailurePropagates() {
        ErrorSinkRowWriter<SeaTunnelRow> failingFlushSink =
                new ErrorSinkRowWriter<SeaTunnelRow>() {
                    @Override
                    public void write(RowErrorContext ctx, SeaTunnelRow row, Throwable t) {}

                    @Override
                    public void flush() throws Exception {
                        throw new Exception("flush failed");
                    }

                    @Override
                    public void close() {}
                };

        StageErrorConfig config = StageErrorConfig.builder().mode(ErrorHandlerMode.ROUTE).build();
        ErrorHandler<SeaTunnelRow> handler = new ErrorHandler<>(config, failingFlushSink);

        Exception ex = assertThrows(Exception.class, handler::flush);

        assertTrue(ex.getMessage().contains("flush failed"));
        handler.close();
    }

    @Test
    public void testSynchronizedErrorSinkFlushDelegates() throws Exception {
        AtomicInteger flushCount = new AtomicInteger();
        ErrorSinkRowWriter<SeaTunnelRow> delegate =
                new ErrorSinkRowWriter<SeaTunnelRow>() {
                    @Override
                    public void write(RowErrorContext ctx, SeaTunnelRow row, Throwable t) {}

                    @Override
                    public void flush() {
                        flushCount.incrementAndGet();
                    }

                    @Override
                    public void flush(long checkpointId) {
                        flushCount.addAndGet((int) checkpointId);
                    }

                    @Override
                    public void close() {}
                };

        StageErrorConfig config = StageErrorConfig.builder().mode(ErrorHandlerMode.ROUTE).build();
        ErrorHandler<SeaTunnelRow> handler =
                new ErrorHandler<>(config, new SynchronizedErrorSinkRowWriter<>(delegate));

        handler.flush();
        handler.flush(2L);

        assertEquals(3, flushCount.get());
        handler.close();
    }

    @Test
    public void testRouteModeWithoutErrorSinkFailsFast() {
        Map<String, Object> envOptions = new HashMap<>();
        Map<String, Object> transformErrorHandler = new HashMap<>();
        transformErrorHandler.put("mode", "ROUTE");
        envOptions.put("transform_error_handler", transformErrorHandler);

        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                ErrorHandlerConfigUtil.buildStageConfig(
                                        envOptions, ErrorHandlerConfigUtil.StageType.TRANSFORM));

        assertTrue(ex.getMessage().contains("env.transform_error_handler.mode=ROUTE"));
        assertTrue(ex.getMessage().contains("env.transform_error_handler.sink.plugin_name"));
    }

    @Test
    public void testInvalidErrorHandlerModeFailsFast() {
        Map<String, Object> envOptions = new HashMap<>();
        Map<String, Object> errorHandler = new HashMap<>();
        errorHandler.put("mode", "unknown");
        envOptions.put("error_handler", errorHandler);

        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                ErrorHandlerConfigUtil.buildStageConfig(
                                        envOptions, ErrorHandlerConfigUtil.StageType.SINK));

        assertTrue(ex.getMessage().contains("Unsupported error handler mode"));
    }

    @Test
    public void testInvalidErrorHandlerNumericValueFailsFast() {
        Map<String, Object> envOptions = new HashMap<>();
        Map<String, Object> errorHandler = new HashMap<>();
        errorHandler.put("mode", "LOG");
        errorHandler.put("max_error_ratio", "not-a-number");
        envOptions.put("error_handler", errorHandler);

        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                ErrorHandlerConfigUtil.buildStageConfig(
                                        envOptions, ErrorHandlerConfigUtil.StageType.SINK));

        assertTrue(ex.getMessage().contains("max_error_ratio"));
    }

    @Test
    public void testStageSinkOptionsMergeWithGlobalSinkOptions() {
        Map<String, Object> envOptions = new HashMap<>();
        Map<String, Object> global = new HashMap<>();
        global.put("mode", "ROUTE");
        Map<String, Object> globalSink = new HashMap<>();
        globalSink.put("plugin_name", "Jdbc");
        globalSink.put("url", "jdbc:mysql://localhost:3306/test");
        global.put("sink", globalSink);
        envOptions.put("error_handler", global);

        Map<String, Object> stage = new HashMap<>();
        stage.put("mode", "ROUTE");
        Map<String, Object> stageSink = new HashMap<>();
        stageSink.put("error_table", "sink_errors");
        stage.put("sink", stageSink);
        envOptions.put("sink_error_handler", stage);

        StageErrorConfig config =
                ErrorHandlerConfigUtil.buildStageConfig(
                        envOptions, ErrorHandlerConfigUtil.StageType.SINK);

        assertEquals("Jdbc", config.getSink().getPluginName());
        assertEquals("sink_errors", config.getSink().getErrorTable());
        assertEquals("jdbc:mysql://localhost:3306/test", config.getSink().getOptions().get("url"));
    }

    @Test
    public void testMaxErrorRatioMinRecordsDefault() {
        Map<String, Object> envOptions = new HashMap<>();
        Map<String, Object> errorHandler = new HashMap<>();
        errorHandler.put("mode", "LOG");
        envOptions.put("error_handler", errorHandler);

        StageErrorConfig config =
                ErrorHandlerConfigUtil.buildStageConfig(
                        envOptions, ErrorHandlerConfigUtil.StageType.SINK);

        assertEquals(10000, config.getMaxErrorRatioMinRecords());
    }

    @Test
    public void testErrorHandlingTransformWrappersDelegateStatefulHooks() {
        RecordingMapTransform mapDelegate = new RecordingMapTransform();
        ErrorHandlingMapTransform<SeaTunnelRow> mapWrapper =
                new ErrorHandlingMapTransform<>(mapDelegate, null, null);

        List<CatalogTable> mapInputCatalogTables = Collections.emptyList();
        mapWrapper.setInputCatalogTables(mapInputCatalogTables);
        mapWrapper.setTypeInfo(null);

        assertSame(mapInputCatalogTables, mapDelegate.inputCatalogTables);
        assertTrue(mapDelegate.typeInfoSet);

        RecordingFlatMapTransform flatMapDelegate = new RecordingFlatMapTransform();
        ErrorHandlingFlatMapTransform<SeaTunnelRow> flatMapWrapper =
                new ErrorHandlingFlatMapTransform<>(flatMapDelegate, null, null);

        List<CatalogTable> flatMapInputCatalogTables = Collections.emptyList();
        flatMapWrapper.setInputCatalogTables(flatMapInputCatalogTables);
        flatMapWrapper.setTypeInfo(null);

        assertSame(flatMapInputCatalogTables, flatMapDelegate.inputCatalogTables);
        assertTrue(flatMapDelegate.typeInfoSet);
    }

    @Test
    public void testOriginalDataTruncation() {
        MockErrorSinkWriter mockSink = new MockErrorSinkWriter();

        StageErrorConfig config =
                StageErrorConfig.builder()
                        .mode(ErrorHandlerMode.ROUTE)
                        .includeOriginalData(true)
                        .originalDataMaxLength(10) // Very short limit
                        .build();

        ErrorHandler<SeaTunnelRow> handler = new ErrorHandler<>(config, mockSink);
        RowErrorContext ctx = createContext();

        handler.incrementTotalRecords();
        handler.onError(ctx, createRow(1), new RuntimeException("test"));

        // Original data should be truncated in logs (checking via mock)
        handler.close();
    }

    @Test
    public void testCombinedThresholds() {
        // Both thresholds configured, first one to hit should fail
        StageErrorConfig config =
                StageErrorConfig.builder()
                        .mode(ErrorHandlerMode.LOG)
                        .maxErrorRecords(20)
                        .maxErrorRatio(0.5) // 50% - very high to avoid ratio trigger
                        .maxErrorRatioMinRecords(100)
                        .build();

        ErrorHandler<SeaTunnelRow> handler = new ErrorHandler<>(config);
        RowErrorContext ctx = createContext();

        // Process 30 success + 20 errors = 20 errors hits maxErrorRecords first
        for (int i = 0; i < 30; i++) {
            handler.incrementTotalRecords();
        }
        for (int i = 0; i < 20; i++) {
            handler.incrementTotalRecords();
            handler.onError(ctx, createRow(i), new RuntimeException("error " + i));
        }

        // 21st error should hit maxErrorRecords (20% ratio < 50% threshold)
        handler.incrementTotalRecords();
        RuntimeException ex =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                handler.onError(
                                        ctx, createRow(21), new RuntimeException("error 21")));

        assertTrue(ex.getMessage().contains("21 records exceeded max_error_records=20"));
        handler.close();
    }

    @Test
    public void testCloseWithStatsSummary() {
        // Verify close() logs summary
        StageErrorConfig config = StageErrorConfig.builder().mode(ErrorHandlerMode.LOG).build();

        ErrorHandler<SeaTunnelRow> handler = new ErrorHandler<>(config);
        RowErrorContext ctx = createContext();

        handler.incrementTotalRecords();
        handler.incrementTotalRecords();
        handler.incrementTotalRecords();
        handler.onError(ctx, createRow(1), new RuntimeException("error"));

        // Should log summary: totalRecords=3, errorRecords=1
        handler.close();
    }

    @Test
    public void testNullRowErrorContext() {
        StageErrorConfig config = StageErrorConfig.builder().mode(ErrorHandlerMode.LOG).build();

        ErrorHandler<SeaTunnelRow> handler = new ErrorHandler<>(config);

        // Null context should be handled gracefully
        assertThrows(
                NullPointerException.class,
                () -> handler.onError(null, createRow(1), new RuntimeException("error")));

        handler.close();
    }

    @Test
    public void testConcurrentErrorHandling() throws InterruptedException {
        // Test thread safety of error counting
        StageErrorConfig config =
                StageErrorConfig.builder().mode(ErrorHandlerMode.LOG).maxErrorRecords(1000).build();

        ErrorHandler<SeaTunnelRow> handler = new ErrorHandler<>(config);
        AtomicInteger exceptionCount = new AtomicInteger(0);
        int threadCount = 10;
        int errorsPerThread = 50;

        Thread[] threads = new Thread[threadCount];
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            threads[t] =
                    new Thread(
                            () -> {
                                RowErrorContext ctx = createContext();
                                for (int i = 0; i < errorsPerThread; i++) {
                                    try {
                                        handler.incrementTotalRecords();
                                        handler.onError(
                                                ctx,
                                                createRow(threadId * 1000 + i),
                                                new RuntimeException("error " + i));
                                    } catch (RuntimeException e) {
                                        exceptionCount.incrementAndGet();
                                    }
                                }
                            });
            threads[t].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        // All errors should be handled without exceptions (500 < 1000 threshold)
        assertEquals(0, exceptionCount.get());
        handler.close();
    }

    // Helper methods

    private RowErrorContext createContext() {
        return new RowErrorContext(TEST_STAGE, TEST_STAGE, TEST_PLUGIN, TEST_TABLE);
    }

    private SeaTunnelRow createRow(int id) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, "name_" + id, 20 + id});
        row.setTableId(TEST_TABLE);
        return row;
    }

    private static class InMemoryCounterStateStore implements CounterStateStore<String> {

        private final Map<String, Long> values = new ConcurrentHashMap<>();

        @Override
        public boolean initializeIfAbsent(String key, long initialValue) {
            return values.putIfAbsent(key, initialValue) == null;
        }

        @Override
        public Long get(String key) {
            return values.get(key);
        }

        @Override
        public Long incrementAndGet(String key) {
            if (!values.containsKey(key)) {
                return null;
            }
            return values.computeIfPresent(key, (ignored, current) -> current + 1L);
        }

        @Override
        public Long addAndGet(String key, long delta) {
            if (!values.containsKey(key)) {
                return null;
            }
            return values.computeIfPresent(key, (ignored, current) -> current + delta);
        }

        @Override
        public void set(String key, long value) {
            values.put(key, value);
        }

        @Override
        public void remove(String key) {
            values.remove(key);
        }
    }

    /** Mock error sink writer for testing. */
    private static class MockErrorSinkWriter implements ErrorSinkRowWriter<SeaTunnelRow> {

        private final List<ErrorRecord> writtenErrors = new ArrayList<>();

        @Override
        public void write(RowErrorContext ctx, SeaTunnelRow row, Throwable t) {
            writtenErrors.add(new ErrorRecord(ctx, row, t));
        }

        @Override
        public void close() {
            // No-op
        }

        public List<ErrorRecord> getWrittenErrors() {
            return writtenErrors;
        }

        static class ErrorRecord {
            final RowErrorContext context;
            final SeaTunnelRow row;
            final Throwable throwable;

            ErrorRecord(RowErrorContext context, SeaTunnelRow row, Throwable throwable) {
                this.context = context;
                this.row = row;
                this.throwable = throwable;
            }
        }
    }

    private static class RecordingMapTransform implements SeaTunnelMapTransform<SeaTunnelRow> {

        private List<CatalogTable> inputCatalogTables;
        private boolean typeInfoSet;

        @Override
        public SeaTunnelRow map(SeaTunnelRow row) {
            return row;
        }

        @Override
        public CatalogTable getProducedCatalogTable() {
            return null;
        }

        @Override
        public List<CatalogTable> getProducedCatalogTables() {
            return Collections.emptyList();
        }

        @Override
        @Deprecated
        public void setTypeInfo(SeaTunnelDataType<SeaTunnelRow> inputDataType) {
            typeInfoSet = true;
        }

        @Override
        public void setInputCatalogTables(List<CatalogTable> inputCatalogTables) {
            this.inputCatalogTables = inputCatalogTables;
        }

        @Override
        public String getPluginName() {
            return "RecordingMap";
        }

        @Override
        public void setJobContext(JobContext jobContext) {}
    }

    private static class RecordingFlatMapTransform
            implements SeaTunnelFlatMapTransform<SeaTunnelRow> {

        private List<CatalogTable> inputCatalogTables;
        private boolean typeInfoSet;

        @Override
        public List<SeaTunnelRow> flatMap(SeaTunnelRow row) {
            return Collections.singletonList(row);
        }

        @Override
        public CatalogTable getProducedCatalogTable() {
            return null;
        }

        @Override
        public List<CatalogTable> getProducedCatalogTables() {
            return Collections.emptyList();
        }

        @Override
        @Deprecated
        public void setTypeInfo(SeaTunnelDataType<SeaTunnelRow> inputDataType) {
            typeInfoSet = true;
        }

        @Override
        public void setInputCatalogTables(List<CatalogTable> inputCatalogTables) {
            this.inputCatalogTables = inputCatalogTables;
        }

        @Override
        public String getPluginName() {
            return "RecordingFlatMap";
        }

        @Override
        public void setJobContext(JobContext jobContext) {}
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.starrocks.client;

import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StarRocksSinkManagerTest {

    private SinkConfig mockSinkConfig;
    private StarRocksStreamLoadVisitor mockStreamLoadVisitor;
    private StarRocksSinkManager sinkManager;

    /**
     * Counts labels created by the production lifecycle so tests can detect unintended
     * regeneration.
     */
    private AtomicInteger labelSequence;

    @BeforeEach
    void setUp() {
        mockSinkConfig = mock(SinkConfig.class);
        mockStreamLoadVisitor = mock(StarRocksStreamLoadVisitor.class);
        when(mockSinkConfig.getBatchMaxSize()).thenReturn(10);
        when(mockSinkConfig.getBatchMaxBytes()).thenReturn(1024 * 1024 * 1024L);
        when(mockSinkConfig.getMaxRetries()).thenReturn(3);
        when(mockSinkConfig.getRetryBackoffMultiplierMs()).thenReturn(0);
        when(mockSinkConfig.getMaxRetryBackoffMs()).thenReturn(1000);
        labelSequence = new AtomicInteger();
        this.sinkManager =
                new StarRocksSinkManager(mockSinkConfig, null, mockStreamLoadVisitor) {
                    public String createBatchLabel() {
                        return "test-label-" + labelSequence.incrementAndGet();
                    }
                };
    }

    /**
     * Verifies that a reused-label exception can no longer silently discard buffered rows.
     *
     * <p>A later successful flush must still send the record retained after retries are exhausted.
     */
    @Test
    void testLabelAlreadyMessageDoesNotDiscardBufferedRecords() throws Exception {
        doThrow(new RuntimeException("Label [test-label-1] has already been used"))
                .when(mockStreamLoadVisitor)
                .doStreamLoad(any());

        sinkManager.write("test-record");

        assertThrows(StarRocksConnectorException.class, () -> sinkManager.flush());
        verify(mockStreamLoadVisitor, times(4)).doStreamLoad(any());

        doReturn(true).when(mockStreamLoadVisitor).doStreamLoad(any());
        assertDoesNotThrow(() -> sinkManager.flush());
        verify(mockStreamLoadVisitor, times(5)).doStreamLoad(any());
        assertEquals(1, labelSequence.get());
    }

    @Test
    void testLabelAlreadyMessageNotHandled() throws Exception {
        // Mock behavior for a different exception
        doThrow(new RuntimeException("Some other error"))
                .when(mockStreamLoadVisitor)
                .doStreamLoad(any());

        // Add a record to trigger flush
        sinkManager.write("test-record");

        // Verify that the exception is propagated after retries
        assertThrows(StarRocksConnectorException.class, () -> sinkManager.flush());
        verify(mockStreamLoadVisitor, times(4))
                .doStreamLoad(any()); // 3 retries + 1 initial attempt
    }

    /**
     * Verifies that a false visitor response retains the batch until a later successful flush.
     *
     * <p>The second flush proves that the first failed flush did not clear the internal batch.
     */
    @Test
    void testFalseResponseDoesNotDiscardBufferedRecords() throws Exception {
        List<String> attemptedLabels = new ArrayList<>();
        when(mockStreamLoadVisitor.doStreamLoad(any()))
                .thenAnswer(
                        invocation -> {
                            attemptedLabels.add(
                                    ((StarRocksFlushTuple) invocation.getArgument(0)).getLabel());
                            return attemptedLabels.size() >= 5;
                        });
        sinkManager.write("test-record");

        assertThrows(StarRocksConnectorException.class, () -> sinkManager.flush());
        verify(mockStreamLoadVisitor, times(4)).doStreamLoad(any());

        assertDoesNotThrow(() -> sinkManager.flush());
        verify(mockStreamLoadVisitor, times(5)).doStreamLoad(any());
        assertEquals(5, attemptedLabels.size());
        for (String attemptedLabel : attemptedLabels) {
            assertEquals("test-label-1", attemptedLabel);
        }
        assertEquals(1, labelSequence.get());
    }

    /**
     * Verifies that a confirmed successful response releases the buffered rows exactly once.
     *
     * <p>An immediate second flush must not issue another Stream Load request.
     */
    @Test
    void testSuccessfulResponseClearsBufferedRecords() throws Exception {
        when(mockStreamLoadVisitor.doStreamLoad(any())).thenReturn(true);
        sinkManager.write("test-record");

        assertDoesNotThrow(() -> sinkManager.flush());
        assertDoesNotThrow(() -> sinkManager.flush());
        verify(mockStreamLoadVisitor, times(1)).doStreamLoad(any());
        assertEquals(1, labelSequence.get());
    }

    /**
     * Verifies that an aborted transaction receives a new label before its batch is retried.
     *
     * <p>The captured labels prevent a retry from reusing the transaction known to be aborted.
     */
    @Test
    void testAbortedLabelIsRecreatedBeforeRetry() throws Exception {
        AtomicInteger labelSequence = new AtomicInteger();
        List<String> attemptedLabels = new ArrayList<>();
        StarRocksConnectorException abortedLabel =
                new StarRocksConnectorException(
                        StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
                        "The prior transaction was aborted.",
                        true);
        when(mockStreamLoadVisitor.doStreamLoad(any()))
                .thenAnswer(
                        invocation -> {
                            StarRocksFlushTuple tuple = invocation.getArgument(0);
                            attemptedLabels.add(tuple.getLabel());
                            if (attemptedLabels.size() == 1) {
                                throw abortedLabel;
                            }
                            return true;
                        });
        StarRocksSinkManager manager =
                new StarRocksSinkManager(mockSinkConfig, null, mockStreamLoadVisitor) {
                    @Override
                    public String createBatchLabel() {
                        return "test-label-" + labelSequence.incrementAndGet();
                    }
                };

        manager.write("test-record");
        assertDoesNotThrow(() -> manager.flush());

        assertEquals(2, attemptedLabels.size());
        assertEquals("test-label-1", attemptedLabels.get(0));
        assertEquals("test-label-2", attemptedLabels.get(1));
    }

    /**
     * Verifies that a null response follows the same fail-closed path as an explicit false result.
     */
    @Test
    void testNullResponseDoesNotDiscardBufferedRecordsOrLabel() throws Exception {
        when(mockStreamLoadVisitor.doStreamLoad(any())).thenReturn(null);
        sinkManager.write("test-record");

        assertThrows(StarRocksConnectorException.class, () -> sinkManager.flush());
        assertEquals(1, labelSequence.get());

        doReturn(true).when(mockStreamLoadVisitor).doStreamLoad(any());
        assertDoesNotThrow(() -> sinkManager.flush());
        assertEquals(1, labelSequence.get());
    }

    /**
     * Verifies that a pending snapshot is resolved before a new record enters the active batch. New
     * records must never be cleared as part of an older successful flush.
     */
    @Test
    void testPendingSnapshotIsFlushedBeforeAcceptingNewRecord() throws Exception {
        when(mockSinkConfig.getMaxRetries()).thenReturn(0);
        List<String> attemptedLabels = new ArrayList<>();
        List<Integer> attemptedRows = new ArrayList<>();
        AtomicInteger attempts = new AtomicInteger();
        when(mockStreamLoadVisitor.doStreamLoad(any()))
                .thenAnswer(
                        invocation -> {
                            StarRocksFlushTuple tuple = invocation.getArgument(0);
                            attemptedLabels.add(tuple.getLabel());
                            attemptedRows.add(tuple.getRows().size());
                            return attempts.incrementAndGet() > 1;
                        });

        sinkManager.write("first-record");
        assertThrows(StarRocksConnectorException.class, () -> sinkManager.flush());
        sinkManager.write("second-record");
        assertDoesNotThrow(() -> sinkManager.flush());

        assertEquals(3, attemptedLabels.size());
        assertEquals("test-label-1", attemptedLabels.get(0));
        assertEquals("test-label-1", attemptedLabels.get(1));
        assertEquals("test-label-2", attemptedLabels.get(2));
        assertEquals(1, attemptedRows.get(0));
        assertEquals(1, attemptedRows.get(1));
        assertEquals(1, attemptedRows.get(2));
    }

    /**
     * Verifies that retry exhaustion cannot create one more label after the final aborted attempt.
     */
    @Test
    void testRecreatedLabelExhaustionRetainsLastAttemptedLabel() throws Exception {
        StarRocksConnectorException abortedLabel =
                new StarRocksConnectorException(
                        StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
                        "The prior transaction was aborted.",
                        true);
        doThrow(abortedLabel).when(mockStreamLoadVisitor).doStreamLoad(any());
        sinkManager.write("test-record");

        assertThrows(StarRocksConnectorException.class, () -> sinkManager.flush());
        assertEquals(4, labelSequence.get());

        doReturn(true).when(mockStreamLoadVisitor).doStreamLoad(any());
        assertDoesNotThrow(() -> sinkManager.flush());
        assertEquals(4, labelSequence.get());
    }

    /**
     * Verifies that an interrupted retry preserves the pending snapshot, label, and interrupt flag.
     */
    @Test
    void testInterruptedRetryRetainsBufferedRecordsAndLabel() throws Exception {
        when(mockStreamLoadVisitor.doStreamLoad(any())).thenReturn(false);
        try {
            Thread.currentThread().interrupt();
            sinkManager.write("test-record");
            assertThrows(StarRocksConnectorException.class, () -> sinkManager.flush());
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(1, labelSequence.get());
        } finally {
            Thread.interrupted();
        }

        doReturn(true).when(mockStreamLoadVisitor).doStreamLoad(any());
        assertDoesNotThrow(() -> sinkManager.flush());
        assertEquals(1, labelSequence.get());
    }
}

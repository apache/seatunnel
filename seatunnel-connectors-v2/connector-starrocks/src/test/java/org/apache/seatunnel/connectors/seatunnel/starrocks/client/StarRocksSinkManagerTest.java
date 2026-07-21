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

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StarRocksSinkManagerTest {

    private SinkConfig mockSinkConfig;
    private StarRocksStreamLoadVisitor mockStreamLoadVisitor;
    private StarRocksSinkManager sinkManager;

    @BeforeEach
    void setUp() {
        mockSinkConfig = mock(SinkConfig.class);
        mockStreamLoadVisitor = mock(StarRocksStreamLoadVisitor.class);
        when(mockSinkConfig.getBatchMaxSize()).thenReturn(10);
        when(mockSinkConfig.getBatchMaxBytes()).thenReturn(1024 * 1024 * 1024L);
        when(mockSinkConfig.getMaxRetries()).thenReturn(3);
        when(mockSinkConfig.getRetryBackoffMultiplierMs()).thenReturn(100);
        when(mockSinkConfig.getMaxRetryBackoffMs()).thenReturn(1000);
        AtomicInteger labelSequence = new AtomicInteger();
        this.sinkManager =
                new StarRocksSinkManager(mockSinkConfig, null, mockStreamLoadVisitor) {
                    public String createBatchLabel() {
                        return "test-label-" + labelSequence.incrementAndGet();
                    }
                };
    }

    @Test
    void testUnclassifiedLabelMessageDoesNotChangeLabel() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        when(mockStreamLoadVisitor.doStreamLoad(any()))
                .thenAnswer(
                        invocation -> {
                            StarRocksFlushTuple tuple = invocation.getArgument(0);
                            if (attempts.incrementAndGet() == 1) {
                                assertEquals("test-label-1", tuple.getLabel());
                                throw new RuntimeException(
                                        "Label [test-label-1] has already been used");
                            }
                            assertEquals("test-label-1", tuple.getLabel());
                            return true;
                        });

        sinkManager.write("test-record");

        assertDoesNotThrow(() -> sinkManager.flush());
        verify(mockStreamLoadVisitor, times(2)).doStreamLoad(any());
    }

    @Test
    void testLabelAlreadyUsedExhaustionPreservesBatchForNextFlush() throws Exception {
        when(mockStreamLoadVisitor.doStreamLoad(any()))
                .thenAnswer(
                        invocation -> {
                            StarRocksFlushTuple tuple = invocation.getArgument(0);
                            throw new RuntimeException(
                                    "Label [" + tuple.getLabel() + "] has already been used");
                        });

        sinkManager.write("test-record");

        assertThrows(StarRocksConnectorException.class, () -> sinkManager.flush());
        verify(mockStreamLoadVisitor, times(4)).doStreamLoad(any());

        org.mockito.Mockito.reset(mockStreamLoadVisitor);
        when(mockStreamLoadVisitor.doStreamLoad(any())).thenReturn(true);

        assertDoesNotThrow(() -> sinkManager.flush());
        verify(mockStreamLoadVisitor, times(1)).doStreamLoad(any());
    }

    @Test
    void testLabelAlreadyUsedWithNoRetriesFailsWithoutDroppingBatch() throws Exception {
        when(mockSinkConfig.getMaxRetries()).thenReturn(0);
        when(mockStreamLoadVisitor.doStreamLoad(any()))
                .thenAnswer(
                        invocation -> {
                            StarRocksFlushTuple tuple = invocation.getArgument(0);
                            throw new RuntimeException(
                                    "Label [" + tuple.getLabel() + "] has already been used");
                        });

        sinkManager.write("test-record");

        assertThrows(StarRocksConnectorException.class, () -> sinkManager.flush());
        verify(mockStreamLoadVisitor, times(1)).doStreamLoad(any());
    }

    @Test
    void testReCreateLabelExceptionRetriesWithNewLabel() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        when(mockStreamLoadVisitor.doStreamLoad(any()))
                .thenAnswer(
                        invocation -> {
                            StarRocksFlushTuple tuple = invocation.getArgument(0);
                            if (attempts.incrementAndGet() == 1) {
                                assertEquals("test-label-1", tuple.getLabel());
                                throw new StarRocksConnectorException(
                                        StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
                                        "The previous label cannot be reused",
                                        true);
                            }
                            assertEquals("test-label-2", tuple.getLabel());
                            return true;
                        });

        sinkManager.write("test-record");

        assertDoesNotThrow(() -> sinkManager.flush());
        verify(mockStreamLoadVisitor, times(2)).doStreamLoad(any());
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
}

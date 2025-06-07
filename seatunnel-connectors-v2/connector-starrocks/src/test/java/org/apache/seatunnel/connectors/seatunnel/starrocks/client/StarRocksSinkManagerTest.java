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
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
        this.sinkManager =
                new StarRocksSinkManager(mockSinkConfig, null, mockStreamLoadVisitor) {
                    public String createBatchLabel() {
                        return "test-label";
                    }
                };
    }

    @Test
    void testLabelAlreadyMessageHandledCorrectly() throws Exception {
        // Mock behavior for label already used
        doThrow(new RuntimeException("Label [test-label] has already been used"))
                .when(mockStreamLoadVisitor)
                .doStreamLoad(any());

        // Add a record to trigger flush
        sinkManager.write("test-record");

        // Verify that the exception is caught and the batch is skipped
        assertDoesNotThrow(() -> sinkManager.flush());
        verify(mockStreamLoadVisitor, times(1)).doStreamLoad(any());
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

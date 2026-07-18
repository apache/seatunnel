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

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StarRocksStreamLoadVisitorTest {

    @Test
    void throwsExceptionWhenBatchMaxBytesExceedsLimitForCSVFormat() {
        SinkConfig sinkConfig = mock(SinkConfig.class);
        when(sinkConfig.getLoadFormat()).thenReturn(SinkConfig.StreamLoadFormat.CSV);
        when(sinkConfig.getBatchMaxBytes()).thenReturn(2147483638L);
        when(sinkConfig.getBatchMaxSize()).thenReturn(100);
        Map<String, Object> props = new HashMap<>();
        props.put("row_delimiter", "\n");
        when(sinkConfig.getStreamLoadProps()).thenReturn(props);

        assertThrows(
                StarRocksConnectorException.class,
                () -> {
                    StarRocksStreamLoadVisitor visitor =
                            new StarRocksStreamLoadVisitor(sinkConfig, mock(TableSchema.class));
                    visitor.checkBatchMaxBytes(2147483638L, 100);
                });
    }

    @Test
    void throwsExceptionWhenBatchMaxBytesExceedsLimitForJSONFormat() {
        SinkConfig sinkConfig = mock(SinkConfig.class);
        when(sinkConfig.getLoadFormat()).thenReturn(SinkConfig.StreamLoadFormat.JSON);
        when(sinkConfig.getBatchMaxBytes()).thenReturn(2147483637L);
        when(sinkConfig.getBatchMaxSize()).thenReturn(100);

        assertThrows(
                StarRocksConnectorException.class,
                () -> {
                    StarRocksStreamLoadVisitor visitor =
                            new StarRocksStreamLoadVisitor(sinkConfig, mock(TableSchema.class));
                    visitor.checkBatchMaxBytes(2147483637L, 100);
                });
    }

    @Test
    void doesNotThrowExceptionWhenBatchMaxBytesWithinLimitForCSVFormat() {
        SinkConfig sinkConfig = mock(SinkConfig.class);
        when(sinkConfig.getLoadFormat()).thenReturn(SinkConfig.StreamLoadFormat.CSV);
        when(sinkConfig.getBatchMaxBytes()).thenReturn(2147483637L);
        when(sinkConfig.getBatchMaxSize()).thenReturn(10);

        Map<String, Object> props = new HashMap<>();
        props.put("row_delimiter", "\n");
        when(sinkConfig.getStreamLoadProps()).thenReturn(props);
        StarRocksStreamLoadVisitor visitor =
                new StarRocksStreamLoadVisitor(sinkConfig, mock(TableSchema.class));

        assertDoesNotThrow(() -> visitor.checkBatchMaxBytes(2147483637L, 10));
    }

    @Test
    void doesNotThrowExceptionWhenBatchMaxBytesWithinLimitForJSONFormat() {
        SinkConfig sinkConfig = mock(SinkConfig.class);
        when(sinkConfig.getLoadFormat()).thenReturn(SinkConfig.StreamLoadFormat.JSON);
        when(sinkConfig.getBatchMaxBytes()).thenReturn(2147483636L);
        when(sinkConfig.getBatchMaxSize()).thenReturn(10);

        StarRocksStreamLoadVisitor visitor =
                new StarRocksStreamLoadVisitor(sinkConfig, mock(TableSchema.class));
        assertDoesNotThrow(() -> visitor.checkBatchMaxBytes(2147483636L, 10));
    }

    @Test
    void throwsExceptionForUnsupportedLoadFormat() {
        SinkConfig sinkConfig = mock(SinkConfig.class);
        when(sinkConfig.getBatchMaxBytes()).thenReturn(1024L);
        when(sinkConfig.getBatchMaxSize()).thenReturn(10);

        assertThrows(
                StarRocksConnectorException.class,
                () -> {
                    StarRocksStreamLoadVisitor visitor =
                            new StarRocksStreamLoadVisitor(sinkConfig, mock(TableSchema.class));
                    visitor.checkBatchMaxBytes(1024, 10);
                });
    }

    @Test
    void treatsVisibleReusedLabelAsSuccessful() throws Exception {
        StarRocksStreamLoadVisitor visitor = visitorWithReusedLabelState("VISIBLE");

        assertTrue(visitor.doStreamLoad(flushTuple()));
    }

    @Test
    void treatsCommittedReusedLabelAsSuccessful() throws Exception {
        StarRocksStreamLoadVisitor visitor = visitorWithReusedLabelState("COMMITTED");

        assertTrue(visitor.doStreamLoad(flushTuple()));
    }

    @Test
    void marksAbortedReusedLabelForRecreation() throws Exception {
        StarRocksStreamLoadVisitor visitor = visitorWithReusedLabelState("ABORTED");

        StarRocksConnectorException exception =
                assertThrows(
                        StarRocksConnectorException.class,
                        () -> visitor.doStreamLoad(flushTuple()));

        assertTrue(exception.needReCreateLabel());
    }

    @Test
    void keepsUnknownReusedLabelFromBeingResubmitted() throws Exception {
        StarRocksStreamLoadVisitor visitor = visitorWithReusedLabelState("UNKNOWN");

        StarRocksConnectorException exception =
                assertThrows(
                        StarRocksConnectorException.class,
                        () -> visitor.doStreamLoad(flushTuple()));

        assertFalse(exception.needReCreateLabel());
    }

    @Test
    void treatsLabelAlreadyExistsWithVisibleStateAsSuccessful() throws Exception {
        StarRocksStreamLoadVisitor visitor =
                visitorWithLoadResultAndLabelState("Label Already Exists", null, "VISIBLE");

        assertTrue(visitor.doStreamLoad(flushTuple()));
    }

    @Test
    void interruptionWhileCheckingLabelStateFailsWithoutRecreatingLabel() throws Exception {
        StarRocksStreamLoadVisitor visitor = visitorWithReusedLabelState("PREPARE");

        Thread.currentThread().interrupt();
        try {
            StarRocksConnectorException exception =
                    assertThrows(
                            StarRocksConnectorException.class,
                            () -> visitor.doStreamLoad(flushTuple()));

            assertFalse(exception.needReCreateLabel());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    /** Builds a visitor whose stream-load response requires checking an existing label state. */
    private StarRocksStreamLoadVisitor visitorWithReusedLabelState(String labelState)
            throws IOException {
        return visitorWithLoadResultAndLabelState(
                "Fail", "Label [test-label] has already been used", labelState);
    }

    /** Builds a visitor with deterministic stream-load and label-state responses. */
    private StarRocksStreamLoadVisitor visitorWithLoadResultAndLabelState(
            String status, String message, String labelState) throws IOException {
        SinkConfig sinkConfig = mock(SinkConfig.class);
        when(sinkConfig.getLoadFormat()).thenReturn(SinkConfig.StreamLoadFormat.JSON);
        when(sinkConfig.getBatchMaxBytes()).thenReturn(1024L);
        when(sinkConfig.getBatchMaxSize()).thenReturn(10);
        when(sinkConfig.getNodeUrls()).thenReturn(Collections.singletonList("localhost:8030"));
        when(sinkConfig.getDatabase()).thenReturn("test_db");
        when(sinkConfig.getTable()).thenReturn("test_table");
        when(sinkConfig.getUsername()).thenReturn("user");
        when(sinkConfig.getPassword()).thenReturn("password");

        TableSchema tableSchema = mock(TableSchema.class);
        when(tableSchema.getColumns()).thenReturn(Collections.emptyList());

        HttpHelper httpHelper = mock(HttpHelper.class);
        when(httpHelper.tryHttpConnection("http://localhost:8030")).thenReturn(true);
        Map<String, Object> loadResult = new HashMap<>();
        loadResult.put("Status", status);
        if (message != null) {
            loadResult.put("Message", message);
        }
        when(httpHelper.doHttpPut(anyString(), any(byte[].class), anyMap())).thenReturn(loadResult);
        when(httpHelper.doHttpGet(anyString(), anyMap()))
                .thenReturn(Collections.singletonMap("state", labelState));

        return new StarRocksStreamLoadVisitor(sinkConfig, tableSchema, httpHelper);
    }

    /** Creates one JSON row under the label used by the response-state tests. */
    private StarRocksFlushTuple flushTuple() {
        byte[] row = "{}".getBytes(StandardCharsets.UTF_8);
        return new StarRocksFlushTuple(
                "test-label", (long) row.length, Collections.singletonList(row));
    }
}

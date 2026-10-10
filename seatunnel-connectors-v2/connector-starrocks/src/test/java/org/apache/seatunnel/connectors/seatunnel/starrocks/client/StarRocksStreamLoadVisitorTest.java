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
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
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

    /**
     * Verifies that Publish Timeout is resolved through the label state instead of being trusted as
     * success.
     *
     * <p>StarRocks documents Publish Timeout as "loaded, publish pending", but a cluster that was
     * just restarted can still abort the pending transaction. Releasing the batch without
     * confirming the label state silently loses it, so the visitor must poll get_load_state and
     * only a VISIBLE/COMMITTED label confirms the load.
     */
    @Test
    void resolvesPublishTimeoutByConfirmingLabelState() throws Exception {
        SinkConfig sinkConfig = createSinkConfig();
        HttpHelper httpHelper = mock(HttpHelper.class);
        when(httpHelper.tryHttpConnection("http://localhost:8030")).thenReturn(true);
        when(httpHelper.doHttpPut(anyString(), any(byte[].class), any()))
                .thenReturn(createLoadResult("Publish Timeout"));
        Map<String, Object> labelState = new HashMap<>();
        labelState.put("state", "VISIBLE");
        when(httpHelper.doHttpGet(anyString(), any(), anyInt())).thenReturn(labelState);
        StarRocksStreamLoadVisitor visitor =
                new StarRocksStreamLoadVisitor(sinkConfig, createTableSchema(), httpHelper);

        assertTrue(visitor.doStreamLoad(createFlushTuple()));
        verify(httpHelper).doHttpGet(anyString(), any(), anyInt());
    }

    /**
     * Verifies that a Publish Timeout label reported ABORTED authorizes a replacement label.
     *
     * <p>An aborted transaction means the batch never became durable, so the manager must resend it
     * under a new label instead of dropping it.
     */
    @Test
    void recreatesLabelWhenPublishTimeoutLabelIsAborted() throws Exception {
        SinkConfig sinkConfig = createSinkConfig();
        HttpHelper httpHelper = mock(HttpHelper.class);
        when(httpHelper.tryHttpConnection("http://localhost:8030")).thenReturn(true);
        when(httpHelper.doHttpPut(anyString(), any(byte[].class), any()))
                .thenReturn(createLoadResult("Publish Timeout"));
        Map<String, Object> labelState = new HashMap<>();
        labelState.put("state", "ABORTED");
        when(httpHelper.doHttpGet(anyString(), any(), anyInt())).thenReturn(labelState);
        StarRocksStreamLoadVisitor visitor =
                new StarRocksStreamLoadVisitor(sinkConfig, createTableSchema(), httpHelper);

        StarRocksConnectorException exception =
                assertThrows(
                        StarRocksConnectorException.class,
                        () -> visitor.doStreamLoad(createFlushTuple()));
        assertTrue(exception.needReCreateLabel());
    }

    /**
     * Verifies that a Publish Timeout label stuck in PREPARE fails closed after the bounded wait.
     *
     * <p>An unresolved outcome must never release the batch: the thrown error lets the manager keep
     * the buffered rows so the job can replay them from its checkpoint.
     */
    @Test
    void failsWhenPublishTimeoutLabelNeverResolves() throws Exception {
        SinkConfig sinkConfig = createSinkConfig();
        HttpHelper httpHelper = mock(HttpHelper.class);
        when(httpHelper.tryHttpConnection("http://localhost:8030")).thenReturn(true);
        when(httpHelper.doHttpPut(anyString(), any(byte[].class), any()))
                .thenReturn(createLoadResult("Publish Timeout"));
        Map<String, Object> labelState = new HashMap<>();
        labelState.put("state", "PREPARE");
        when(httpHelper.doHttpGet(anyString(), any(), anyInt())).thenReturn(labelState);
        StarRocksStreamLoadVisitor visitor =
                new StarRocksStreamLoadVisitor(sinkConfig, createTableSchema(), httpHelper, 500);

        StarRocksConnectorException exception =
                assertThrows(
                        StarRocksConnectorException.class,
                        () -> visitor.doStreamLoad(createFlushTuple()));
        assertFalse(exception.needReCreateLabel());
        verify(httpHelper, atLeastOnce()).doHttpGet(anyString(), any(), anyInt());
    }

    /**
     * Verifies that an existing visible label confirms the original batch was loaded.
     *
     * <p>The visitor must query the label state instead of resending the batch under a new label.
     */
    @Test
    void returnsSuccessWhenExistingLabelIsVisible() throws Exception {
        SinkConfig sinkConfig = createSinkConfig();
        HttpHelper httpHelper = mock(HttpHelper.class);
        when(httpHelper.tryHttpConnection("http://localhost:8030")).thenReturn(true);
        when(httpHelper.doHttpPut(anyString(), any(byte[].class), any()))
                .thenReturn(createLoadResult("Label Already Exists"));
        Map<String, Object> labelState = new HashMap<>();
        labelState.put("state", "VISIBLE");
        when(httpHelper.doHttpGet(anyString(), any(), anyInt())).thenReturn(labelState);
        StarRocksStreamLoadVisitor visitor =
                new StarRocksStreamLoadVisitor(sinkConfig, createTableSchema(), httpHelper);

        assertTrue(visitor.doStreamLoad(createFlushTuple()));
        verify(httpHelper).doHttpGet(anyString(), any(), anyInt());
    }

    /**
     * Verifies that a new label is allowed only after StarRocks reports the old one aborted.
     *
     * <p>The recreation flag is the manager's proof that retrying cannot duplicate a committed
     * load.
     */
    @Test
    void recreatesLabelOnlyAfterExistingLabelIsAborted() throws Exception {
        SinkConfig sinkConfig = createSinkConfig();
        HttpHelper httpHelper = mock(HttpHelper.class);
        when(httpHelper.tryHttpConnection("http://localhost:8030")).thenReturn(true);
        Map<String, Object> loadResult = createLoadResult("Fail");
        loadResult.put("Message", "Label [test-label] has already been used");
        when(httpHelper.doHttpPut(anyString(), any(byte[].class), any())).thenReturn(loadResult);
        Map<String, Object> labelState = new HashMap<>();
        labelState.put("state", "ABORTED");
        when(httpHelper.doHttpGet(anyString(), any(), anyInt())).thenReturn(labelState);
        StarRocksStreamLoadVisitor visitor =
                new StarRocksStreamLoadVisitor(sinkConfig, createTableSchema(), httpHelper);

        StarRocksConnectorException exception =
                assertThrows(
                        StarRocksConnectorException.class,
                        () -> visitor.doStreamLoad(createFlushTuple()));
        assertTrue(exception.needReCreateLabel());
    }

    /**
     * Verifies that interruption fails the flush without treating the label check as success.
     *
     * <p>The interrupt flag must remain set so the engine can preserve cancellation semantics.
     */
    @Test
    void failsWithoutDiscardingStateWhenLabelCheckIsInterrupted() throws Exception {
        SinkConfig sinkConfig = createSinkConfig();
        HttpHelper httpHelper = mock(HttpHelper.class);
        when(httpHelper.tryHttpConnection("http://localhost:8030")).thenReturn(true);
        when(httpHelper.doHttpPut(anyString(), any(byte[].class), any()))
                .thenReturn(createLoadResult("Label Already Exists"));
        Map<String, Object> labelState = new HashMap<>();
        labelState.put("state", "PREPARE");
        when(httpHelper.doHttpGet(anyString(), any(), anyInt())).thenReturn(labelState);
        StarRocksStreamLoadVisitor visitor =
                new StarRocksStreamLoadVisitor(sinkConfig, createTableSchema(), httpHelper);

        try {
            Thread.currentThread().interrupt();
            assertThrows(
                    StarRocksConnectorException.class,
                    () -> visitor.doStreamLoad(createFlushTuple()));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    /**
     * Verifies that the normal Stream Load success response is accepted without a label query. A
     * committed request must complete through the shortest response path.
     */
    @Test
    void returnsSuccessForNormalSuccessStatus() throws Exception {
        SinkConfig sinkConfig = createSinkConfig();
        HttpHelper httpHelper = mock(HttpHelper.class);
        when(httpHelper.tryHttpConnection("http://localhost:8030")).thenReturn(true);
        when(httpHelper.doHttpPut(anyString(), any(byte[].class), any()))
                .thenReturn(createLoadResult("Success"));
        StarRocksStreamLoadVisitor visitor =
                new StarRocksStreamLoadVisitor(sinkConfig, createTableSchema(), httpHelper);

        assertTrue(visitor.doStreamLoad(createFlushTuple()));
        verify(httpHelper, never()).doHttpGet(anyString(), any(), anyInt());
    }

    /**
     * Verifies that an existing committed label confirms the original batch without resubmission.
     */
    @Test
    void returnsSuccessWhenExistingLabelIsCommitted() throws Exception {
        SinkConfig sinkConfig = createSinkConfig();
        HttpHelper httpHelper = mock(HttpHelper.class);
        when(httpHelper.tryHttpConnection("http://localhost:8030")).thenReturn(true);
        when(httpHelper.doHttpPut(anyString(), any(byte[].class), any()))
                .thenReturn(createLoadResult("Label Already Exists"));
        Map<String, Object> labelState = new HashMap<>();
        labelState.put("state", "COMMITTED");
        when(httpHelper.doHttpGet(anyString(), any(), anyInt())).thenReturn(labelState);
        StarRocksStreamLoadVisitor visitor =
                new StarRocksStreamLoadVisitor(sinkConfig, createTableSchema(), httpHelper);

        assertTrue(visitor.doStreamLoad(createFlushTuple()));
    }

    /**
     * Verifies that an UNKNOWN label state is polled until the deadline before failing closed.
     *
     * <p>A front-end recovering from a restart can report UNKNOWN for a committed label, so an
     * immediate failure would cause needless replay; the state must be retried inside the bounded
     * wait and still never authorize a replacement label.
     */
    @Test
    void failsWhenExistingLabelStateIsUnknown() throws Exception {
        SinkConfig sinkConfig = createSinkConfig();
        HttpHelper httpHelper = mock(HttpHelper.class);
        when(httpHelper.tryHttpConnection("http://localhost:8030")).thenReturn(true);
        when(httpHelper.doHttpPut(anyString(), any(byte[].class), any()))
                .thenReturn(createLoadResult("Label Already Exists"));
        Map<String, Object> labelState = new HashMap<>();
        labelState.put("state", "UNKNOWN");
        when(httpHelper.doHttpGet(anyString(), any(), anyInt())).thenReturn(labelState);
        StarRocksStreamLoadVisitor visitor =
                new StarRocksStreamLoadVisitor(sinkConfig, createTableSchema(), httpHelper, 500);

        StarRocksConnectorException exception =
                assertThrows(
                        StarRocksConnectorException.class,
                        () -> visitor.doStreamLoad(createFlushTuple()));
        assertFalse(exception.needReCreateLabel());
        verify(httpHelper, atLeastOnce()).doHttpGet(anyString(), any(), anyInt());
    }

    /**
     * Verifies that an unrecognized response status is resolved through the label state.
     *
     * <p>Any 200 response with a Status field means StarRocks accepted the request, so its
     * transaction exists and can be confirmed; trusting an unknown status string would either drop
     * or duplicate the batch.
     */
    @Test
    void resolvesUnexpectedStreamLoadStatusThroughLabelState() throws Exception {
        SinkConfig sinkConfig = createSinkConfig();
        HttpHelper httpHelper = mock(HttpHelper.class);
        when(httpHelper.tryHttpConnection("http://localhost:8030")).thenReturn(true);
        when(httpHelper.doHttpPut(anyString(), any(byte[].class), any()))
                .thenReturn(createLoadResult("Mystery"));
        Map<String, Object> labelState = new HashMap<>();
        labelState.put("state", "VISIBLE");
        when(httpHelper.doHttpGet(anyString(), any(), anyInt())).thenReturn(labelState);
        StarRocksStreamLoadVisitor visitor =
                new StarRocksStreamLoadVisitor(sinkConfig, createTableSchema(), httpHelper);

        assertTrue(visitor.doStreamLoad(createFlushTuple()));
        verify(httpHelper, atLeastOnce()).doHttpGet(anyString(), any(), anyInt());
    }

    /**
     * Verifies that an ordinary failed load does not authorize a new idempotency label. Only an
     * explicitly aborted transaction may release the pending batch for a replacement label.
     */
    @Test
    void failsOrdinaryLoadFailureWithoutRecreatingLabel() throws Exception {
        SinkConfig sinkConfig = createSinkConfig();
        HttpHelper httpHelper = mock(HttpHelper.class);
        when(httpHelper.tryHttpConnection("http://localhost:8030")).thenReturn(true);
        Map<String, Object> loadResult = createLoadResult("Fail");
        loadResult.put("Message", "Invalid row format");
        when(httpHelper.doHttpPut(anyString(), any(byte[].class), any())).thenReturn(loadResult);
        StarRocksStreamLoadVisitor visitor =
                new StarRocksStreamLoadVisitor(sinkConfig, createTableSchema(), httpHelper);

        StarRocksConnectorException exception =
                assertThrows(
                        StarRocksConnectorException.class,
                        () -> visitor.doStreamLoad(createFlushTuple()));
        assertFalse(exception.needReCreateLabel());
    }

    /**
     * Verifies that a PREPARE transaction can become visible before the bounded wait expires. The
     * original label must be accepted without submitting the batch again.
     */
    @Test
    void returnsSuccessWhenPrepareStateBecomesVisible() throws Exception {
        SinkConfig sinkConfig = createSinkConfig();
        HttpHelper httpHelper = mock(HttpHelper.class);
        when(httpHelper.tryHttpConnection("http://localhost:8030")).thenReturn(true);
        when(httpHelper.doHttpPut(anyString(), any(byte[].class), any()))
                .thenReturn(createLoadResult("Label Already Exists"));
        Map<String, Object> preparing = new HashMap<>();
        preparing.put("state", "PREPARE");
        Map<String, Object> visible = new HashMap<>();
        visible.put("state", "VISIBLE");
        when(httpHelper.doHttpGet(anyString(), any(), anyInt())).thenReturn(preparing, visible);
        StarRocksStreamLoadVisitor visitor =
                new StarRocksStreamLoadVisitor(sinkConfig, createTableSchema(), httpHelper, 2000);

        assertTrue(visitor.doStreamLoad(createFlushTuple()));
        verify(httpHelper, org.mockito.Mockito.times(2)).doHttpGet(anyString(), any(), anyInt());
    }

    /**
     * Verifies that a PREPARE transaction can become committed before the bounded wait expires. A
     * committed label is a terminal success even before the data becomes visible.
     */
    @Test
    void returnsSuccessWhenPrepareStateBecomesCommitted() throws Exception {
        SinkConfig sinkConfig = createSinkConfig();
        HttpHelper httpHelper = mock(HttpHelper.class);
        when(httpHelper.tryHttpConnection("http://localhost:8030")).thenReturn(true);
        when(httpHelper.doHttpPut(anyString(), any(byte[].class), any()))
                .thenReturn(createLoadResult("Label Already Exists"));
        Map<String, Object> preparing = new HashMap<>();
        preparing.put("state", "PREPARE");
        Map<String, Object> committed = new HashMap<>();
        committed.put("state", "COMMITTED");
        when(httpHelper.doHttpGet(anyString(), any(), anyInt())).thenReturn(preparing, committed);
        StarRocksStreamLoadVisitor visitor =
                new StarRocksStreamLoadVisitor(sinkConfig, createTableSchema(), httpHelper, 2000);

        assertTrue(visitor.doStreamLoad(createFlushTuple()));
        verify(httpHelper, org.mockito.Mockito.times(2)).doHttpGet(anyString(), any(), anyInt());
    }

    /**
     * Verifies that a label permanently in PREPARE fails when the total wait deadline expires. The
     * caller can then retry the same pending batch without losing its idempotency label.
     */
    @Test
    void failsWhenPrepareStateExceedsDeadline() throws Exception {
        SinkConfig sinkConfig = createSinkConfig();
        HttpHelper httpHelper = mock(HttpHelper.class);
        when(httpHelper.tryHttpConnection("http://localhost:8030")).thenReturn(true);
        when(httpHelper.doHttpPut(anyString(), any(byte[].class), any()))
                .thenReturn(createLoadResult("Label Already Exists"));
        Map<String, Object> preparing = new HashMap<>();
        preparing.put("state", "PREPARE");
        when(httpHelper.doHttpGet(anyString(), any(), anyInt())).thenReturn(preparing);
        StarRocksStreamLoadVisitor visitor =
                new StarRocksStreamLoadVisitor(sinkConfig, createTableSchema(), httpHelper, 500);

        assertThrows(
                StarRocksConnectorException.class, () -> visitor.doStreamLoad(createFlushTuple()));
        verify(httpHelper, atLeastOnce()).doHttpGet(anyString(), any(), anyInt());
    }

    /**
     * Verifies that the {@code label_state_timeout_ms} sink configuration drives the polling
     * deadline when the visitor is built through the production constructor.
     *
     * <p>The bounded assertion keeps the test fast even if a regression falls back to the built-in
     * default deadline.
     */
    @Test
    void usesConfiguredLabelStateTimeoutFromSinkConfig() throws Exception {
        SinkConfig sinkConfig = createSinkConfig();
        when(sinkConfig.getLabelStateTimeoutMs()).thenReturn(300L);
        HttpHelper httpHelper = mock(HttpHelper.class);
        when(httpHelper.tryHttpConnection("http://localhost:8030")).thenReturn(true);
        when(httpHelper.doHttpPut(anyString(), any(byte[].class), any()))
                .thenReturn(createLoadResult("Publish Timeout"));
        Map<String, Object> labelState = new HashMap<>();
        labelState.put("state", "PREPARE");
        when(httpHelper.doHttpGet(anyString(), any(), anyInt())).thenReturn(labelState);
        StarRocksStreamLoadVisitor visitor =
                new StarRocksStreamLoadVisitor(sinkConfig, createTableSchema(), httpHelper);

        StarRocksConnectorException exception =
                assertTimeoutPreemptively(
                        Duration.ofSeconds(10),
                        () ->
                                assertThrows(
                                        StarRocksConnectorException.class,
                                        () -> visitor.doStreamLoad(createFlushTuple())));
        assertFalse(exception.needReCreateLabel());
        verify(httpHelper, atLeastOnce()).doHttpGet(anyString(), any(), anyInt());
    }

    /**
     * Verifies that a transient failure while reading the label state is retried inside the bounded
     * wait instead of failing the flush immediately.
     */
    @Test
    void retriesLabelStateCheckWhenHttpGetFails() throws Exception {
        SinkConfig sinkConfig = createSinkConfig();
        HttpHelper httpHelper = mock(HttpHelper.class);
        when(httpHelper.tryHttpConnection("http://localhost:8030")).thenReturn(true);
        when(httpHelper.doHttpPut(anyString(), any(byte[].class), any()))
                .thenReturn(createLoadResult("Publish Timeout"));
        Map<String, Object> labelState = new HashMap<>();
        labelState.put("state", "VISIBLE");
        when(httpHelper.doHttpGet(anyString(), any(), anyInt()))
                .thenThrow(new IOException("connection reset"))
                .thenReturn(labelState);
        StarRocksStreamLoadVisitor visitor =
                new StarRocksStreamLoadVisitor(sinkConfig, createTableSchema(), httpHelper, 2000);

        assertTrue(visitor.doStreamLoad(createFlushTuple()));
        verify(httpHelper, org.mockito.Mockito.times(2)).doHttpGet(anyString(), any(), anyInt());
    }

    /**
     * Creates the minimal deterministic sink configuration needed by response-handling tests.
     *
     * @return mocked sink configuration for one JSON Stream Load target
     */
    private SinkConfig createSinkConfig() {
        SinkConfig sinkConfig = mock(SinkConfig.class);
        when(sinkConfig.getLoadFormat()).thenReturn(SinkConfig.StreamLoadFormat.JSON);
        when(sinkConfig.getBatchMaxBytes()).thenReturn(1024L);
        when(sinkConfig.getBatchMaxSize()).thenReturn(10);
        when(sinkConfig.getNodeUrls()).thenReturn(Collections.singletonList("localhost:8030"));
        when(sinkConfig.getDatabase()).thenReturn("test_db");
        when(sinkConfig.getTable()).thenReturn("test_table");
        when(sinkConfig.getUsername()).thenReturn("test_user");
        when(sinkConfig.getPassword()).thenReturn("test_password");
        when(sinkConfig.getStreamLoadProps()).thenReturn(Collections.emptyMap());
        return sinkConfig;
    }

    /**
     * Creates a schema without columns because JSON response tests do not require column headers.
     */
    private TableSchema createTableSchema() {
        TableSchema tableSchema = mock(TableSchema.class);
        when(tableSchema.getColumns()).thenReturn(Collections.emptyList());
        return tableSchema;
    }

    /**
     * Creates one stable JSON row with a fixed label for Stream Load response tests.
     *
     * @return flush tuple whose label can be matched against StarRocks responses
     */
    private StarRocksFlushTuple createFlushTuple() {
        byte[] row = "{}".getBytes(StandardCharsets.UTF_8);
        List<byte[]> rows = Collections.singletonList(row);
        return new StarRocksFlushTuple("test-label", (long) row.length, rows);
    }

    /**
     * Creates a Stream Load response map containing the supplied StarRocks status.
     *
     * @param status response status returned by StarRocks
     * @return mutable response map for test-specific fields
     */
    private Map<String, Object> createLoadResult(String status) {
        Map<String, Object> loadResult = new HashMap<>();
        loadResult.put("Status", status);
        return loadResult;
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.prometheus.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.function.RunnableWithException;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.prometheus.Exception.PrometheusConnectorException;

import org.apache.http.HttpStatus;
import org.apache.http.entity.ByteArrayEntity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PrometheusWriterTest {

    /**
     * The writer should opt in to engine-level timer flush by registering a flush action, and that
     * action should send the buffered records only when the engine invokes it (the flush signal),
     * not on every write.
     */
    @Test
    void shouldRegisterFlushActionAndFlushBufferedRecordsOnSignal() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        HttpResponse ok = new HttpResponse(HttpStatus.SC_NO_CONTENT);
        ArgumentCaptor<RunnableWithException> actionCaptor =
                ArgumentCaptor.forClass(RunnableWithException.class);

        try (MockedConstruction<HttpClientProvider> ignored =
                mockConstruction(
                        HttpClientProvider.class,
                        (mockClient, ctx) ->
                                when(mockClient.doPost(
                                                anyString(), any(), any(ByteArrayEntity.class)))
                                        .thenReturn(ok))) {

            PrometheusWriter writer = createWriter(context);
            writer.write(newPoint());

            verify(context, times(1)).registerFlushAction(actionCaptor.capture());
            // Buffered only: nothing is sent until the engine delivers a flush signal.
            verify(writer.httpClient, never())
                    .doPost(anyString(), any(), any(ByteArrayEntity.class));

            actionCaptor.getValue().run();

            verify(writer.httpClient, times(1))
                    .doPost(anyString(), any(), any(ByteArrayEntity.class));
        }
    }

    /**
     * A flush that does not succeed must be propagated to the engine instead of being silently
     * treated as a successful flush.
     */
    @Test
    void shouldPropagateFlushFailure() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        HttpResponse failed = new HttpResponse(HttpStatus.SC_BAD_REQUEST, "boom");
        ArgumentCaptor<RunnableWithException> actionCaptor =
                ArgumentCaptor.forClass(RunnableWithException.class);

        try (MockedConstruction<HttpClientProvider> ignored =
                mockConstruction(
                        HttpClientProvider.class,
                        (mockClient, ctx) ->
                                when(mockClient.doPost(
                                                anyString(), any(), any(ByteArrayEntity.class)))
                                        .thenReturn(failed))) {

            PrometheusWriter writer = createWriter(context);
            writer.write(newPoint());

            verify(context, times(1)).registerFlushAction(actionCaptor.capture());
            Assertions.assertThrows(
                    PrometheusConnectorException.class, () -> actionCaptor.getValue().run());
        }
    }

    /**
     * On Spark and Flink the sink writer context does not implement registerFlushAction (it keeps
     * the interface's no-op default), so the engine never invokes the flush action. The buffered
     * records must still be delivered when the writer is closed, not lost.
     */
    @Test
    void shouldFlushOnCloseWhenEngineNeverInvokesFlushAction() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        HttpResponse ok = new HttpResponse(HttpStatus.SC_NO_CONTENT);

        try (MockedConstruction<HttpClientProvider> ignored =
                mockConstruction(
                        HttpClientProvider.class,
                        (mockClient, ctx) ->
                                when(mockClient.doPost(
                                                anyString(), any(), any(ByteArrayEntity.class)))
                                        .thenReturn(ok))) {

            PrometheusWriter writer = createWriter(context);
            writer.write(newPoint());

            // Simulate Spark/Flink: the registered flush action is never invoked by the engine.
            verify(writer.httpClient, never())
                    .doPost(anyString(), any(), any(ByteArrayEntity.class));

            writer.close();

            // close() must flush the buffered row so it is not lost.
            verify(writer.httpClient, times(1))
                    .doPost(anyString(), any(), any(ByteArrayEntity.class));
        }
    }

    /**
     * On Spark and Flink the engine never invokes the flush action, but prepareCommit() runs on
     * every checkpoint. The buffered records must be delivered at checkpoint time instead of
     * waiting for batch_size or close(), which bounds the buffered window to one checkpoint
     * interval on those engines and matches the sibling FlushSignal sinks.
     */
    @Test
    void shouldFlushOnPrepareCommitWhenEngineNeverInvokesFlushAction() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        HttpResponse ok = new HttpResponse(HttpStatus.SC_NO_CONTENT);

        try (MockedConstruction<HttpClientProvider> ignored =
                mockConstruction(
                        HttpClientProvider.class,
                        (mockClient, ctx) ->
                                when(mockClient.doPost(
                                                anyString(), any(), any(ByteArrayEntity.class)))
                                        .thenReturn(ok))) {

            PrometheusWriter writer = createWriter(context);
            writer.write(newPoint());

            // Simulate Spark/Flink: the registered flush action is never invoked by the engine.
            verify(writer.httpClient, never())
                    .doPost(anyString(), any(), any(ByteArrayEntity.class));

            Assertions.assertEquals(Optional.empty(), writer.prepareCommit());

            // prepareCommit() must flush the buffered row so it reaches Prometheus at checkpoint.
            verify(writer.httpClient, times(1))
                    .doPost(anyString(), any(), any(ByteArrayEntity.class));

            // The buffer must be cleared after the flush, so a later checkpoint with no new rows
            // does not re-deliver the same row: a second prepareCommit() sends nothing more.
            Assertions.assertEquals(Optional.empty(), writer.prepareCommit());
            verify(writer.httpClient, times(1))
                    .doPost(anyString(), any(), any(ByteArrayEntity.class));
        }
    }

    /**
     * When the final flush in close() fails and closing the HTTP client also throws, the meaningful
     * flush failure must be the exception that surfaces, with the client-close error suppressed,
     * not the other way around.
     */
    @Test
    void closeShouldKeepFlushExceptionWhenHttpClientCloseAlsoThrows() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        HttpResponse failed = new HttpResponse(HttpStatus.SC_BAD_REQUEST, "boom");

        try (MockedConstruction<HttpClientProvider> ignored =
                mockConstruction(
                        HttpClientProvider.class,
                        (mockClient, ctx) ->
                                when(mockClient.doPost(
                                                anyString(), any(), any(ByteArrayEntity.class)))
                                        .thenReturn(failed))) {

            PrometheusWriter writer = createWriter(context);
            writer.write(newPoint());
            // The HTTP client teardown also fails during close().
            doThrow(new IOException("client close failed")).when(writer.httpClient).close();

            PrometheusConnectorException thrown =
                    Assertions.assertThrows(PrometheusConnectorException.class, writer::close);

            boolean clientCloseSuppressed = false;
            for (Throwable suppressed : thrown.getSuppressed()) {
                if (suppressed instanceof IOException) {
                    clientCloseSuppressed = true;
                }
            }
            Assertions.assertTrue(
                    clientCloseSuppressed,
                    "The client-close IOException should be suppressed on the flush failure");
        }
    }

    /** A retryable failure (HTTP 5xx) is retried, and a following success delivers the batch. */
    @Test
    void shouldRetryRetryableFailureThenSucceed() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        HttpResponse unavailable =
                new HttpResponse(HttpStatus.SC_SERVICE_UNAVAILABLE, "unavailable");
        HttpResponse ok = new HttpResponse(HttpStatus.SC_NO_CONTENT);

        try (MockedConstruction<HttpClientProvider> ignored =
                mockConstruction(
                        HttpClientProvider.class,
                        (mockClient, ctx) ->
                                when(mockClient.doPost(
                                                anyString(), any(), any(ByteArrayEntity.class)))
                                        .thenReturn(unavailable, ok))) {

            PrometheusWriter writer = createWriter(context, 3);
            writer.write(newPoint());

            // First attempt is 503 (retryable), second is 204: no exception, batch delivered.
            writer.prepareCommit();
            verify(writer.httpClient, times(2))
                    .doPost(anyString(), any(), any(ByteArrayEntity.class));

            // Buffer cleared after delivery: a second flush sends nothing more.
            writer.prepareCommit();
            verify(writer.httpClient, times(2))
                    .doPost(anyString(), any(), any(ByteArrayEntity.class));
        }
    }

    /** After the retries are exhausted, the flush throws and the batch is not cleared. */
    @Test
    void shouldThrowAfterRetriesExhausted() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        HttpResponse unavailable =
                new HttpResponse(HttpStatus.SC_SERVICE_UNAVAILABLE, "unavailable");

        try (MockedConstruction<HttpClientProvider> ignored =
                mockConstruction(
                        HttpClientProvider.class,
                        (mockClient, ctx) ->
                                when(mockClient.doPost(
                                                anyString(), any(), any(ByteArrayEntity.class)))
                                        .thenReturn(unavailable))) {

            PrometheusWriter writer = createWriter(context, 3);
            writer.write(newPoint());

            Assertions.assertThrows(
                    PrometheusConnectorException.class, () -> writer.prepareCommit());
            // retry is the total attempt budget, so retry=3 means 3 doPost calls.
            verify(writer.httpClient, times(3))
                    .doPost(anyString(), any(), any(ByteArrayEntity.class));
        }
    }

    /**
     * After the retries are exhausted the batch stays buffered (not cleared), so a later flush
     * re-sends the same records. This pins down the at-least-once guarantee explicitly.
     */
    @Test
    void shouldRetainBufferAfterRetriesExhausted() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        HttpResponse unavailable =
                new HttpResponse(HttpStatus.SC_SERVICE_UNAVAILABLE, "unavailable");
        HttpResponse ok = new HttpResponse(HttpStatus.SC_NO_CONTENT);

        try (MockedConstruction<HttpClientProvider> ignored =
                mockConstruction(
                        HttpClientProvider.class,
                        (mockClient, ctx) ->
                                when(mockClient.doPost(
                                                anyString(), any(), any(ByteArrayEntity.class)))
                                        .thenReturn(unavailable, ok))) {

            // retry=1 means a single attempt, so the first flush exhausts immediately.
            PrometheusWriter writer = createWriter(context, 1);
            writer.write(newPoint());

            Assertions.assertThrows(
                    PrometheusConnectorException.class, () -> writer.prepareCommit());
            verify(writer.httpClient, times(1))
                    .doPost(anyString(), any(), any(ByteArrayEntity.class));

            // The row is still buffered: a second flush re-sends it and now succeeds.
            writer.prepareCommit();
            verify(writer.httpClient, times(2))
                    .doPost(anyString(), any(), any(ByteArrayEntity.class));
        }
    }

    /** A non-retryable response (other 4xx) fails fast without retrying. */
    @Test
    void shouldFailFastOnNonRetryable4xx() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        HttpResponse forbidden = new HttpResponse(HttpStatus.SC_FORBIDDEN, "forbidden");

        try (MockedConstruction<HttpClientProvider> ignored =
                mockConstruction(
                        HttpClientProvider.class,
                        (mockClient, ctx) ->
                                when(mockClient.doPost(
                                                anyString(), any(), any(ByteArrayEntity.class)))
                                        .thenReturn(forbidden))) {

            PrometheusWriter writer = createWriter(context, 5);
            writer.write(newPoint());

            Assertions.assertThrows(
                    PrometheusConnectorException.class, () -> writer.prepareCommit());
            // Non-retryable: exactly one attempt despite retry=5.
            verify(writer.httpClient, times(1))
                    .doPost(anyString(), any(), any(ByteArrayEntity.class));
        }
    }

    /**
     * A 400 the receiver reports as a duplicate/out-of-order sample is treated as delivered, so a
     * replay after restore does not fail the checkpoint or loop the job.
     */
    @Test
    void shouldTreatDuplicateOrOutOfOrder400AsDelivered() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        HttpResponse duplicate =
                new HttpResponse(HttpStatus.SC_BAD_REQUEST, "duplicate sample for timestamp");

        try (MockedConstruction<HttpClientProvider> ignored =
                mockConstruction(
                        HttpClientProvider.class,
                        (mockClient, ctx) ->
                                when(mockClient.doPost(
                                                anyString(), any(), any(ByteArrayEntity.class)))
                                        .thenReturn(duplicate))) {

            PrometheusWriter writer = createWriter(context, 3);
            writer.write(newPoint());

            // No throw: the duplicate rejection is tolerated as delivered.
            writer.prepareCommit();
            verify(writer.httpClient, times(1))
                    .doPost(anyString(), any(), any(ByteArrayEntity.class));

            // And the buffer is cleared, so a second flush sends nothing more.
            writer.prepareCommit();
            verify(writer.httpClient, times(1))
                    .doPost(anyString(), any(), any(ByteArrayEntity.class));
        }
    }

    private PrometheusWriter createWriter(SinkWriter.Context context) {
        return createWriter(context, 0);
    }

    private PrometheusWriter createWriter(SinkWriter.Context context, int retry) {
        HttpParameter httpParameter = new HttpParameter();
        httpParameter.setUrl("http://localhost:9090/api/v1/write");
        httpParameter.setHeaders(new HashMap<>());
        httpParameter.setRetry(retry);
        // No backoff sleep in tests so retries run instantly and deterministically.
        httpParameter.setRetryBackoffMultiplierMillis(0);
        httpParameter.setRetryBackoffMaxMillis(0);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        // batch_size is not set, so it resolves to its declared default of 1024; each test writes a
        // single row, which stays well below that, so no size-triggered flush occurs.
        ReadonlyConfig pluginConfig = ReadonlyConfig.fromMap(new HashMap<>());
        return new PrometheusWriter(rowType, httpParameter, pluginConfig, context);
    }

    private Point newPoint() {
        Map<String, String> metric = new HashMap<>();
        metric.put("__name__", "test_metric");
        return Point.builder().metric(metric).value(1.0).timestamp(1_600_000_000_000L).build();
    }
}

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

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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

    private PrometheusWriter createWriter(SinkWriter.Context context) {
        HttpParameter httpParameter = new HttpParameter();
        httpParameter.setUrl("http://localhost:9090/api/v1/write");
        httpParameter.setHeaders(new HashMap<>());
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE});
        // No batch_size configured, so writes buffer without auto-flushing.
        ReadonlyConfig pluginConfig = ReadonlyConfig.fromMap(new HashMap<>());
        return new PrometheusWriter(rowType, httpParameter, pluginConfig, context);
    }

    private Point newPoint() {
        Map<String, String> metric = new HashMap<>();
        metric.put("__name__", "test_metric");
        return Point.builder().metric(metric).value(1.0).timestamp(1_600_000_000_000L).build();
    }
}

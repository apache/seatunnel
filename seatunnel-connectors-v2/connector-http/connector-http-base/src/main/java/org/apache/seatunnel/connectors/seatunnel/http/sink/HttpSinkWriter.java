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

package org.apache.seatunnel.connectors.seatunnel.http.sink;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Slf4j
public class HttpSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void>
        implements SupportMultiTableSinkWriter<Void> {
    protected final HttpClientProvider httpClient;
    protected final SeaTunnelRowType seaTunnelRowType;
    protected final HttpParameter httpParameter;
    protected final SerializationSchema serializationSchema;

    // Batch related fields
    private final boolean arrayMode;
    private final int batchSize;
    private final int requestIntervalMs;
    private final List<SeaTunnelRow> batchBuffer;
    private long lastRequestTime;

    public HttpSinkWriter(SeaTunnelRowType seaTunnelRowType, HttpParameter httpParameter) {
        this(seaTunnelRowType, httpParameter, new JsonSerializationSchema(seaTunnelRowType));
    }

    public HttpSinkWriter(
            SeaTunnelRowType seaTunnelRowType,
            HttpParameter httpParameter,
            SerializationSchema serializationSchema) {
        this(
                seaTunnelRowType,
                httpParameter,
                serializationSchema,
                httpParameter.isArrayMode(),
                httpParameter.getBatchSize(),
                httpParameter.getRequestIntervalMs());
    }

    public HttpSinkWriter(
            SeaTunnelRowType seaTunnelRowType,
            HttpParameter httpParameter,
            SerializationSchema serializationSchema,
            boolean arrayMode,
            int batchSize,
            int requestIntervalMs) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.httpParameter = httpParameter;
        this.httpClient = createHttpClient(httpParameter);
        this.serializationSchema = serializationSchema;
        this.arrayMode = arrayMode;
        this.batchSize = batchSize;
        this.requestIntervalMs = requestIntervalMs;
        this.batchBuffer = new ArrayList<>(batchSize);
        this.lastRequestTime = System.currentTimeMillis();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        if (!arrayMode) {
            writeSingleRecord(element);
        } else {
            batchBuffer.add(element);
            if (batchBuffer.size() >= batchSize) {
                flush();
            }
        }
    }

    private void writeSingleRecord(SeaTunnelRow element) throws IOException {
        byte[] serialize = serializationSchema.serialize(element);
        String body = new String(serialize);
        doHttpRequest(body);
    }

    private void flush() throws IOException {
        if (batchBuffer.isEmpty()) {
            return;
        }
        long currentTime = System.currentTimeMillis();
        long timeSinceLastRequest = currentTime - lastRequestTime;
        if (requestIntervalMs > 0 && timeSinceLastRequest < requestIntervalMs) {
            try {
                Thread.sleep(requestIntervalMs - timeSinceLastRequest);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // Array mode: serialize batch data as JSON
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode arrayNode = mapper.createArrayNode();
        for (SeaTunnelRow row : batchBuffer) {
            byte[] serialize = serializationSchema.serialize(row);
            arrayNode.add(new String(serialize));
        }
        String body = mapper.writeValueAsString(arrayNode);
        doHttpRequest(body);

        batchBuffer.clear();
        lastRequestTime = System.currentTimeMillis();
    }

    private void doHttpRequest(String body) {
        try {
            HttpResponse response =
                    httpClient.doPost(httpParameter.getUrl(), httpParameter.getHeaders(), body);
            if (HttpResponse.STATUS_OK == response.getCode()) {
                return;
            }
            log.error(
                    "http client execute exception, http response status code:[{}], content:[{}]",
                    response.getCode(),
                    response.getContent());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        if (arrayMode) {
            flush();
        }
        if (Objects.nonNull(httpClient)) {
            httpClient.close();
        }
    }

    @Override
    public Optional<Void> prepareCommit() {
        if (arrayMode) {
            try {
                flush();
            } catch (IOException e) {
                throw new RuntimeException("Failed to flush data in prepareCommit", e);
            }
        }
        return Optional.empty();
    }

    @VisibleForTesting
    protected HttpClientProvider createHttpClient(HttpParameter httpParameter) {
        return new HttpClientProvider(httpParameter);
    }
}

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

@Slf4j
public abstract class HttpSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void>
        implements SupportMultiTableSinkWriter<Void> {
    protected final HttpClientProvider httpClient;
    protected final SeaTunnelRowType seaTunnelRowType;
    protected final HttpParameter httpParameter;
    protected final SerializationSchema serializationSchema;

    // Batch related fields
    private final boolean arrayMode;
    private final int batchSize;
    private final int requestIntervalMs;
    private final String format;
    private final List<SeaTunnelRow> batchBuffer;
    private long lastRequestTime;

    public HttpSinkWriter(SeaTunnelRowType seaTunnelRowType, HttpParameter httpParameter) {
        this(seaTunnelRowType, httpParameter, new JsonSerializationSchema(seaTunnelRowType));
    }

    public HttpSinkWriter(
            SeaTunnelRowType seaTunnelRowType,
            HttpParameter httpParameter,
            SerializationSchema serializationSchema) {
        this(seaTunnelRowType, httpParameter, serializationSchema, false, 1, 0, "json");
    }

    public HttpSinkWriter(
            SeaTunnelRowType seaTunnelRowType,
            HttpParameter httpParameter,
            boolean arrayMode,
            int batchSize,
            int requestIntervalMs,
            String format) {
        this(
                seaTunnelRowType,
                httpParameter,
                new JsonSerializationSchema(seaTunnelRowType),
                arrayMode,
                batchSize,
                requestIntervalMs,
                format);
    }

    public HttpSinkWriter(
            SeaTunnelRowType seaTunnelRowType,
            HttpParameter httpParameter,
            SerializationSchema serializationSchema,
            boolean arrayMode,
            int batchSize,
            int requestIntervalMs,
            String format) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.httpParameter = httpParameter;
        this.httpClient = createHttpClient(httpParameter);
        this.serializationSchema = serializationSchema;
        this.arrayMode = arrayMode;
        this.batchSize = batchSize;
        this.requestIntervalMs = requestIntervalMs;
        this.format = format;
        this.batchBuffer = new ArrayList<>(batchSize);
        this.lastRequestTime = System.currentTimeMillis();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        if (!arrayMode) {
            // Object mode: send each record individually, ignore batch_size setting
            writeSingleRecord(element);
        } else {
            // Array mode: batch processing
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

        // Check request interval
        long currentTime = System.currentTimeMillis();
        long timeSinceLastRequest = currentTime - lastRequestTime;
        if (requestIntervalMs > 0 && timeSinceLastRequest < requestIntervalMs) {
            try {
                Thread.sleep(requestIntervalMs - timeSinceLastRequest);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Sleep interrupted", e);
            }
        }

        // Array mode: serialize batch data
        if ("json".equalsIgnoreCase(format)) {
            // Constructing JSON arrays
            List<String> jsonRecords = new ArrayList<>(batchBuffer.size());
            for (SeaTunnelRow row : batchBuffer) {
                byte[] serialize = serializationSchema.serialize(row);
                jsonRecords.add(new String(serialize));
            }
            String body = "[" + String.join(",", jsonRecords) + "]";
            doHttpRequest(body);
        } else {
            log.warn("Unsupported format: {}, fallback to sending records one by one", format);
            for (SeaTunnelRow row : batchBuffer) {
                writeSingleRecord(row);
            }
        }

        batchBuffer.clear();
        lastRequestTime = System.currentTimeMillis();
    }

    private void doHttpRequest(String body) {
        try {
            // Send HTTP request
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
            flush(); // Ensure that all data in the buffer is sent out before shutdown
        }
        if (Objects.nonNull(httpClient)) {
            httpClient.close();
        }
    }

    protected abstract HttpClientProvider createHttpClient(HttpParameter httpParameter);
}

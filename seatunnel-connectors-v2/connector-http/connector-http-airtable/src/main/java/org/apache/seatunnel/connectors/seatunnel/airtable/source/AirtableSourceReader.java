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

package org.apache.seatunnel.connectors.seatunnel.airtable.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.JsonField;
import org.apache.seatunnel.connectors.seatunnel.http.config.PageInfo;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceReader;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AirtableSourceReader extends HttpSourceReader {

    private static final int STATUS_TOO_MANY_REQUESTS = 429;
    private static final long MAX_BACKOFF_MILLIS = 300000L;

    private final int requestIntervalMs;
    private final int rateLimitBackoffMs;
    private final int rateLimitMaxRetries;
    private long lastRequestTimeMillis = 0L;

    public AirtableSourceReader(
            HttpParameter httpParameter,
            SingleSplitReaderContext context,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            JsonField jsonField,
            String contentJson,
            PageInfo pageInfo,
            int requestIntervalMs,
            int rateLimitBackoffMs,
            int rateLimitMaxRetries) {
        super(httpParameter, context, deserializationSchema, jsonField, contentJson, pageInfo);
        this.requestIntervalMs = Math.max(0, requestIntervalMs);
        this.rateLimitBackoffMs = Math.max(0, rateLimitBackoffMs);
        this.rateLimitMaxRetries = Math.max(0, rateLimitMaxRetries);
    }

    @Override
    protected HttpResponse executeRequest() throws Exception {
        int retryCount = 0;
        while (true) {
            waitForRequestSlot();
            HttpResponse response = doExecuteRequest();
            if (response.getCode() == STATUS_TOO_MANY_REQUESTS
                    && retryCount < rateLimitMaxRetries) {
                retryCount += 1;
                long backoffMillis = calculateBackoffMillis(retryCount);
                log.warn(
                        "Airtable API rate limit reached, retry {}/{} after {} ms",
                        retryCount,
                        rateLimitMaxRetries,
                        backoffMillis);
                try {
                    Thread.sleep(backoffMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                continue;
            }
            return response;
        }
    }

    private HttpResponse doExecuteRequest() throws Exception {
        return httpClient.execute(
                this.httpParameter.getUrl(),
                this.httpParameter.getMethod().getMethod(),
                this.httpParameter.getHeaders(),
                this.httpParameter.getParams(),
                this.httpParameter.getBody(),
                this.httpParameter.isKeepParamsAsForm());
    }

    private void waitForRequestSlot() {
        if (requestIntervalMs <= 0) {
            return;
        }
        long now = System.currentTimeMillis();
        long elapsed = now - lastRequestTimeMillis;
        if (elapsed < requestIntervalMs) {
            try {
                Thread.sleep(requestIntervalMs - elapsed);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        lastRequestTimeMillis = System.currentTimeMillis();
    }

    private long calculateBackoffMillis(int retryCount) {
        if (rateLimitBackoffMs <= 0) {
            return 0L;
        }
        long exponential = 1L << Math.min(20, Math.max(0, retryCount - 1));
        long waitMillis = rateLimitBackoffMs * exponential;
        return Math.min(waitMillis, MAX_BACKOFF_MILLIS);
    }
}

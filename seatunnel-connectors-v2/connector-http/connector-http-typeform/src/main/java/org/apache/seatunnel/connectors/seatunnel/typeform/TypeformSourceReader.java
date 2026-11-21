/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.typeform;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.JsonField;
import org.apache.seatunnel.connectors.seatunnel.http.config.PageInfo;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceReader;

import lombok.extern.slf4j.Slf4j;

/**
 * Reader that re-implements executeRequest to keep requests at least 500 ms apart (Typeform allows
 * two requests per second per account) and to retry HTTP 429 and 5xx with exponential backoff
 * before handing the response to the base reader.
 */
@Slf4j
public class TypeformSourceReader extends HttpSourceReader {

    static final long DEFAULT_MIN_REQUEST_INTERVAL_MILLIS = 500;

    private long minRequestIntervalMillis = DEFAULT_MIN_REQUEST_INTERVAL_MILLIS;
    private long lastRequestNanos = -1;

    public TypeformSourceReader(
            HttpParameter httpParameter,
            SingleSplitReaderContext readerContext,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            JsonField jsonField,
            String contentField,
            PageInfo pageInfo) {
        super(
                httpParameter,
                readerContext,
                deserializationSchema,
                jsonField,
                contentField,
                pageInfo);
    }

    /** Lets tests shorten the pacing interval. */
    void setMinRequestIntervalMillis(long minRequestIntervalMillis) {
        this.minRequestIntervalMillis = minRequestIntervalMillis;
    }

    @Override
    protected HttpResponse executeRequest() throws Exception {
        int maxRetries = httpParameter.getRetry();
        long waitMillis = httpParameter.getRetryBackoffMultiplierMillis();
        long maxWaitMillis = httpParameter.getRetryBackoffMaxMillis();
        int attempt = 1;
        while (true) {
            waitForRateLimit();
            HttpResponse response = super.executeRequest();
            int code = response.getCode();
            boolean retryable = code == 429 || (code >= 500 && code < 600);
            if (!retryable) {
                return response;
            }
            if (attempt > maxRetries) {
                throw new HttpConnectorException(
                        HttpConnectorErrorCode.REQUEST_FAILED,
                        String.format(
                                "Typeform request failed with HTTP %d after %d attempt(s) (retry=%d)",
                                code, attempt, maxRetries));
            }
            log.warn(
                    "Typeform returned HTTP {}; retry {}/{} in {} ms",
                    code,
                    attempt,
                    maxRetries,
                    waitMillis);

            try {
                Thread.sleep(waitMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(
                        String.format(
                                "Interrupted while backing off after Typeform HTTP %d (attempt %d)",
                                code, attempt),
                        e);
            }
            waitMillis = Math.min(waitMillis * 2, maxWaitMillis);
            attempt++;
        }
    }

    private void waitForRateLimit() {
        if (lastRequestNanos >= 0) {
            long elapsedMillis = (System.nanoTime() - lastRequestNanos) / 1_000_000L;
            long remaining = minRequestIntervalMillis - elapsedMillis;
            if (remaining > 0) {
                try {
                    Thread.sleep(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(
                            "Interrupted while pacing requests to the Typeform API", e);
                }
            }
        }
        lastRequestNanos = System.nanoTime();
    }
}

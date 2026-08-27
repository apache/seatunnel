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

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.JsonField;
import org.apache.seatunnel.connectors.seatunnel.http.config.PageInfo;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceReader;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ThreadLocalRandom;

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

    @VisibleForTesting
    long calculateBackoffMillis(int retryCount) {
        if (rateLimitBackoffMs <= 0) {
            return 0L;
        }
        long exponential = 1L << Math.min(20, Math.max(0, retryCount - 1));
        long waitMillis = Math.min(rateLimitBackoffMs * exponential, MAX_BACKOFF_MILLIS);

        // Spread the delay by adding a random amount on top of it. Without this
        // the delay is a pure function of the retry count, so every reader and
        // writer that hits the rate limit at the same moment retries at the same
        // instants and the burst that caused the 429 reforms on each attempt.
        //
        // The jitter is added rather than centred so the wait is never shorter
        // than rateLimitBackoffMs asked for: this fires on 429, so retrying
        // sooner than configured would work against the setting's purpose. The
        // result stays capped at MAX_BACKOFF_MILLIS.
        long extra = Math.min(waitMillis, MAX_BACKOFF_MILLIS - waitMillis);
        if (extra > 0) {
            return waitMillis + ThreadLocalRandom.current().nextLong(extra + 1);
        }

        // Once the wait reaches MAX_BACKOFF_MILLIS there is no headroom left to
        // add into, so every retry past that point would come back unjittered
        // and the callers would be back in lockstep exactly when the rate limit
        // is at its most persistent. Spread the wait downwards instead. The cap
        // is an upper bound rather than a target, so drawing below it breaks
        // nothing.
        //
        // The floor is the last scheduled wait that still fitted under the cap,
        // or half the wait when the very first retry is already capped. Flooring
        // there keeps the minimum from dropping as the schedule crosses the cap:
        // half of MAX can be less than the previous retry's wait, which would let
        // a later retry sleep for less than an earlier one. It also keeps the
        // wait at or above rateLimitBackoffMs for free, since the last uncapped
        // wait is never smaller than the configured backoff.
        long floor = waitMillis / 2;
        for (long scheduled = rateLimitBackoffMs; scheduled < MAX_BACKOFF_MILLIS; scheduled <<= 1) {
            if (scheduled > floor) {
                floor = scheduled;
            }
        }
        return waitMillis - ThreadLocalRandom.current().nextLong(waitMillis - floor + 1);
    }
}

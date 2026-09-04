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

package org.apache.seatunnel.connectors.seatunnel.stripe.source;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;
import org.apache.seatunnel.connectors.seatunnel.stripe.source.config.StripeSourceParameter;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class StripeSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private static final int HTTP_TOO_MANY_REQUESTS = 429;
    private static final int MAX_ERROR_BODY_LENGTH = 1024;
    private static final long MAX_RATE_LIMIT_BACKOFF_MS = 60000L;

    private final StripeSourceParameter sourceParameter;
    private final SingleSplitReaderContext context;
    private final Sleeper sleeper;
    private HttpClientProvider httpClient;

    public StripeSourceReader(
            StripeSourceParameter sourceParameter, SingleSplitReaderContext context) {
        this(sourceParameter, context, Thread::sleep);
    }

    @VisibleForTesting
    StripeSourceReader(
            StripeSourceParameter sourceParameter,
            SingleSplitReaderContext context,
            Sleeper sleeper) {
        this.sourceParameter = sourceParameter;
        this.context = context;
        this.sleeper = sleeper;
    }

    @Override
    public void open() {
        httpClient = new HttpClientProvider(sourceParameter);
    }

    @Override
    public void close() throws IOException {
        if (httpClient != null) {
            httpClient.close();
        }
    }

    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws Exception {
        String cursor = null;
        Set<String> seenCursors = new HashSet<>();
        do {
            sourceParameter.setStartingAfter(cursor);
            StripePage page = fetchPage(seenCursors);
            for (String paymentIntent : page.paymentIntents) {
                output.collect(new SeaTunnelRow(new Object[] {paymentIntent}));
            }
            cursor = page.nextCursor;
        } while (cursor != null);
        context.signalNoMoreElement();
    }

    private StripePage fetchPage(Set<String> seenCursors) throws Exception {
        HttpResponse response = executeWithRateLimitRetry();
        if (response.getCode() < 200 || response.getCode() > 299) {
            throw requestFailed(response);
        }

        JsonNode root;
        try {
            root = JsonUtils.stringToJsonNode(response.getContent());
        } catch (RuntimeException e) {
            throw new HttpConnectorException(
                    HttpConnectorErrorCode.REQUEST_FAILED,
                    "Stripe PaymentIntents response is not valid JSON",
                    e);
        }
        JsonNode dataNode = root.get("data");
        JsonNode hasMoreNode = root.get("has_more");
        if (!(dataNode instanceof ArrayNode) || hasMoreNode == null || !hasMoreNode.isBoolean()) {
            throw new HttpConnectorException(
                    HttpConnectorErrorCode.REQUEST_FAILED,
                    "Stripe PaymentIntents response must contain array 'data' and boolean 'has_more'");
        }

        ArrayNode data = (ArrayNode) dataNode;
        List<String> paymentIntents = new ArrayList<>(data.size());
        String lastId = null;
        for (JsonNode paymentIntent : data) {
            JsonNode idNode = paymentIntent.get("id");
            if (!paymentIntent.isObject()
                    || idNode == null
                    || !idNode.isTextual()
                    || idNode.textValue().isEmpty()) {
                throw new HttpConnectorException(
                        HttpConnectorErrorCode.REQUEST_FAILED,
                        "Every Stripe PaymentIntent must be an object with a non-empty string 'id'");
            }
            lastId = idNode.textValue();
            paymentIntents.add(paymentIntent.toString());
        }

        String nextCursor = null;
        if (hasMoreNode.booleanValue()) {
            if (lastId == null) {
                throw new HttpConnectorException(
                        HttpConnectorErrorCode.REQUEST_FAILED,
                        "Stripe returned has_more=true with an empty PaymentIntents page");
            }
            if (!seenCursors.add(lastId)) {
                throw new HttpConnectorException(
                        HttpConnectorErrorCode.REQUEST_FAILED,
                        "Stripe pagination repeated cursor '" + lastId + "'");
            }
            nextCursor = lastId;
        }
        return new StripePage(paymentIntents, nextCursor);
    }

    private HttpResponse executeWithRateLimitRetry() throws Exception {
        int retries = 0;
        while (true) {
            HttpResponse response =
                    httpClient.execute(
                            sourceParameter.getUrl(),
                            sourceParameter.getMethod().getMethod(),
                            sourceParameter.getHeaders(),
                            sourceParameter.getParams(),
                            sourceParameter.getBody(),
                            sourceParameter.isKeepParamsAsForm());
            if (response.getCode() != HTTP_TOO_MANY_REQUESTS
                    || retries >= sourceParameter.getRateLimitMaxRetries()) {
                return response;
            }
            retries++;
            long backoffMillis = calculateBackoffMillis(retries);
            log.warn(
                    "Stripe API rate limit reached, retry {}/{} after {} ms",
                    retries,
                    sourceParameter.getRateLimitMaxRetries(),
                    backoffMillis);
            try {
                sleeper.sleep(backoffMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new HttpConnectorException(
                        HttpConnectorErrorCode.REQUEST_FAILED,
                        "Interrupted while waiting to retry Stripe request",
                        e);
            }
        }
    }

    @VisibleForTesting
    long calculateBackoffMillis(int retryNumber) {
        if (sourceParameter.getRateLimitBackoffMs() == 0) {
            return 0L;
        }
        int exponent = Math.min(20, Math.max(0, retryNumber - 1));
        long multiplier = 1L << exponent;
        long base = sourceParameter.getRateLimitBackoffMs();
        if (base > MAX_RATE_LIMIT_BACKOFF_MS / multiplier) {
            return MAX_RATE_LIMIT_BACKOFF_MS;
        }
        return Math.min(base * multiplier, MAX_RATE_LIMIT_BACKOFF_MS);
    }

    private HttpConnectorException requestFailed(HttpResponse response) {
        String responseBody = response.getContent();
        if (responseBody == null) {
            responseBody = "";
        } else {
            String authorization = sourceParameter.getHeaders().get("Authorization");
            if (authorization != null && authorization.startsWith("Bearer ")) {
                String secretKey = authorization.substring("Bearer ".length());
                if (!secretKey.isEmpty()) {
                    responseBody = responseBody.replace(secretKey, "[REDACTED]");
                }
            }
        }
        if (responseBody.length() > MAX_ERROR_BODY_LENGTH) {
            responseBody = responseBody.substring(0, MAX_ERROR_BODY_LENGTH) + "...";
        }
        return new HttpConnectorException(
                HttpConnectorErrorCode.REQUEST_FAILED,
                "Stripe PaymentIntents request failed with HTTP "
                        + response.getCode()
                        + ": "
                        + responseBody);
    }

    @FunctionalInterface
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private static final class StripePage {
        private final List<String> paymentIntents;
        private final String nextCursor;

        private StripePage(List<String> paymentIntents, String nextCursor) {
            this.paymentIntents = paymentIntents;
            this.nextCursor = nextCursor;
        }
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.google.analytics4;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4Report.failure;

final class GoogleAnalytics4SourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final GoogleAnalytics4Config config;
    private final SingleSplitReaderContext context;
    private final Object cancellation = new Object();
    private volatile boolean closed;
    private volatile GoogleAnalytics4HttpTransport transport;
    private GoogleCredentials credentials;

    GoogleAnalytics4SourceReader(GoogleAnalytics4Config config, SingleSplitReaderContext context) {
        this(config, context, null);
    }

    GoogleAnalytics4SourceReader(
            GoogleAnalytics4Config config,
            SingleSplitReaderContext context,
            GoogleAnalytics4HttpTransport transport) {
        this.config = config;
        this.context = context;
        this.transport = transport;
    }

    @Override
    public void open() throws IOException {
        synchronized (cancellation) {
            if (closed) {
                throw failure("reader is closed");
            }
            if (transport == null) {
                transport = new GoogleAnalytics4HttpTransport(config);
            }
        }
        try {
            if (config.getKeyFile() != null) {
                try (InputStream file = Files.newInputStream(Paths.get(config.getKeyFile()))) {
                    byte[] key = GoogleAnalytics4HttpTransport.readBounded(file, 1048576);
                    ServiceAccountCredentials account =
                            ServiceAccountCredentials.fromStream(
                                    new ByteArrayInputStream(key), () -> transport);
                    if (!"https://oauth2.googleapis.com/token"
                            .equals(account.getTokenServerUri().toString())) {
                        throw failure("service account token_uri must use Google's OAuth endpoint");
                    }
                    credentials =
                            account.createWithCustomRetryStrategy(false)
                                    .createScoped(
                                            Collections.singleton(
                                                    "https://www.googleapis.com/auth/analytics.readonly"));
                }
            }
        } catch (IOException | RuntimeException e) {
            close();
            throw failure(
                    "cannot load service_account_key_file; use a trusted service account JSON file with the standard token_uri");
        }
    }

    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws Exception {
        long deadline =
                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.getReportTimeout());
        transport.setReportDeadline(deadline);
        GoogleAnalytics4Report report = new GoogleAnalytics4Report(config);
        long offset = 0;
        while (!closed) {
            transport.checkActive();
            byte[] response = fetch(report.request(offset), deadline);
            GoogleAnalytics4Report.Page page = report.parse(response, offset);
            for (SeaTunnelRow row : page.rows) {
                transport.checkActive();
                output.collect(row);
            }
            offset += page.rows.size();
            if (page.finished) {
                transport.checkActive();
                context.signalNoMoreElement();
                return;
            }
        }
        throw failure("reader cancelled");
    }

    private byte[] fetch(byte[] request, long deadline) throws IOException {
        boolean refreshed = false;
        int retry = 0;
        while (true) {
            transport.checkActive();
            String token = token(false);
            GoogleAnalytics4HttpTransport.Response response;
            try {
                response =
                        transport.report(
                                config.getEndpoint()
                                        + "/v1beta/properties/"
                                        + config.getPropertyId()
                                        + ":runReport",
                                request,
                                token);
            } catch (GoogleAnalytics4HttpTransport.ResponseLimitException e) {
                throw e;
            } catch (IOException e) {
                transport.checkActive();
                if (retry >= config.getRetries()) {
                    throw e;
                }
                awaitRetry(retry++, null, deadline);
                continue;
            }
            if (response.status == 200) {
                return response.body;
            }
            if (response.status == 401 && credentials != null && !refreshed) {
                token(true);
                refreshed = true;
                continue;
            }
            boolean transientStatus =
                    response.status == 429
                            || response.status == 500
                            || response.status == 502
                            || response.status == 503
                            || response.status == 504;
            if (!transientStatus || retry >= config.getRetries()) {
                throw failure(
                        "runReport failed with HTTP "
                                + response.status
                                + "; check request compatibility, property access or quota (response body redacted)");
            }
            awaitRetry(retry++, response.retryAfter, deadline);
        }
    }

    private String token(boolean force) throws IOException {
        if (credentials == null) {
            return null;
        }
        try {
            AccessToken token = credentials.getAccessToken();
            if (force
                    || token == null
                    || token.getExpirationTime() == null
                    || token.getExpirationTime().getTime() <= System.currentTimeMillis() + 60000) {
                // Synchronous refresh; no background refresh executor or hidden page retry.
                credentials.refresh();
                token = credentials.getAccessToken();
            }
            transport.checkActive();
            if (token == null
                    || token.getTokenValue() == null
                    || token.getTokenValue().isEmpty()
                    || !token.getTokenValue().matches("[A-Za-z0-9._~+/=-]+")) {
                throw failure("invalid access token");
            }
            return token.getTokenValue();
        } catch (IOException | RuntimeException e) {
            throw failure("authentication refresh failed (details redacted)");
        }
    }

    private void awaitRetry(int attempt, String retryAfter, long deadline) throws IOException {
        long base = Math.min(config.getBackoff(), 1000L << attempt);
        long jittered = ThreadLocalRandom.current().nextLong(Math.max(1, base / 2), base + 1);
        long wait =
                retryDelay(retryAfter, jittered, config.getBackoff(), System.currentTimeMillis());
        if (TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()) <= wait) {
            throw failure("retry would exceed report_timeout_ms");
        }
        long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(wait);
        synchronized (cancellation) {
            while (!closed && System.nanoTime() < until) {
                try {
                    TimeUnit.NANOSECONDS.timedWait(cancellation, until - System.nanoTime());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw failure("retry interrupted");
                }
            }
        }
        transport.checkActive();
    }

    static long retryDelay(String header, long fallback, long maximum, long now)
            throws IOException {
        long wait = fallback;
        if (header != null) {
            try {
                String value = header.trim();
                if (value.matches("[0-9]+")) {
                    wait = Math.max(wait, Math.multiplyExact(Long.parseLong(value), 1000L));
                } else {
                    wait =
                            Math.max(
                                    wait,
                                    ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
                                                    .toInstant()
                                                    .toEpochMilli()
                                            - now);
                }
            } catch (NumberFormatException | ArithmeticException | DateTimeParseException e) {
                throw failure("invalid Retry-After header");
            }
        }
        if (wait > maximum) {
            throw failure("Retry-After exceeds max_retry_wait_ms; retry the job later");
        }
        return wait;
    }

    @Override
    public void close() throws IOException {
        synchronized (cancellation) {
            closed = true;
            cancellation.notifyAll();
            if (transport != null) {
                transport.close();
            }
        }
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.datadog;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.io.JsonStringEncoder;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Validated settings shared by the master and worker, with credentials excluded from checkpoints.
 */
final class DatadogConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Set<String> SITES =
            new HashSet<>(
                    Arrays.asList(
                            "datadoghq.com",
                            "us3.datadoghq.com",
                            "us5.datadoghq.com",
                            "datadoghq.eu",
                            "ap1.datadoghq.com",
                            "ap2.datadoghq.com",
                            "uk1.datadoghq.com",
                            "ddog-gov.com",
                            "us2.ddog-gov.com"));
    private static final Duration MAX_RANGE = Duration.ofDays(36525);

    // Source configuration carries credentials to the worker; checkpoint splits carry only scope.
    final String accessKey;
    final String secretKey;
    // Only an allow-listed API origin may receive authenticated requests.
    final String host;
    final DatadogResource resource;
    // Event bounds are absent for catalogs; an implicit end is frozen by the coordinator.
    final Instant start;
    final Instant explicitEnd;
    final int sliceDays;
    final String logsQuery;
    final int metricsWindow;
    // The requested size is clamped once to the selected endpoint limit and bound into scope.
    final int pageSize;
    // Socket/connection idle timeout and whole-page elapsed budget are distinct bounds.
    final int timeout;
    final int deadline;
    final int attempts;
    final int maxBytes;
    // Query-defining settings are hashed; transport budgets can change during compatible restore.
    private final byte[] scope;

    DatadogConfig(ReadonlyConfig options) {
        accessKey = readKey(options, DatadogSourceOptions.ACCESS_KEY);
        secretKey = readKey(options, DatadogSourceOptions.SECRET_KEY);
        resource = DatadogResource.parse(options.get(DatadogSourceOptions.RESOURCE));
        String site = options.get(DatadogSourceOptions.SITE);
        if (!SITES.contains(site)) {
            throw new IllegalArgumentException("Unsupported Datadog site.");
        }
        host = "api." + site;
        start = instant(options, DatadogSourceOptions.START_TIME);
        explicitEnd = instant(options, DatadogSourceOptions.END_TIME);
        logsQuery = options.getOptional(DatadogSourceOptions.LOGS_QUERY).orElse("");
        if ((resource.dated && start == null)
                || (!resource.dated && (start != null || explicitEnd != null))) {
            throw new IllegalArgumentException(
                    "Datadog event scans require start_time. Time bounds apply only to logs and audit_logs.");
        }
        if (logsQuery.length() > 4096
                || (resource != DatadogResource.LOGS && !logsQuery.isEmpty())) {
            throw new IllegalArgumentException(
                    "logs_query applies to logs only and is limited to 4096 characters.");
        }
        sliceDays = bounded(options, DatadogSourceOptions.SLICE_DAYS, 1, 31);
        metricsWindow = bounded(options, DatadogSourceOptions.METRICS_WINDOW_SECONDS, 0, 5184000);
        pageSize =
                Math.min(
                        resource.maxPageSize,
                        bounded(options, DatadogSourceOptions.PAGE_SIZE, 1, 1000));
        timeout = bounded(options, DatadogSourceOptions.REQUEST_TIMEOUT_MS, 100, 60000);
        deadline = bounded(options, DatadogSourceOptions.PAGE_DEADLINE_MS, 100, 300000);
        attempts = bounded(options, DatadogSourceOptions.MAX_ATTEMPTS, 1, 8);
        maxBytes = bounded(options, DatadogSourceOptions.MAX_RESPONSE_BYTES, 1024, 16777216);
        if (explicitEnd != null) {
            validateEnd(explicitEnd);
        }
        // The identity binds the query and account to a restored cursor without storing keys.
        scope =
                digest(
                        String.join(
                                "\n",
                                host,
                                resource.name(),
                                accessKey,
                                secretKey,
                                escape(logsQuery),
                                String.valueOf(start),
                                String.valueOf(explicitEnd),
                                Integer.toString(sliceDays),
                                Integer.toString(pageSize),
                                Integer.toString(metricsWindow)));
    }

    /**
     * Called on the coordinator so all recovery attempts reuse one automatically selected end.
     * Independent jobs create their own bounds rather than sharing a scheduler watermark.
     */
    DatadogSplit initialSplit(Instant now) {
        Instant end = resource.dated ? (explicitEnd == null ? now : explicitEnd) : null;
        if (end != null) {
            validateEnd(end);
        }
        return new DatadogSplit(scope.clone(), start, end);
    }

    /**
     * Fail before any request when restored progress belongs to another query or state version.
     * Transport-only budgets do not change the identity of the source query.
     */
    void verify(DatadogSplit split) {
        if (!split.matches(scope)) {
            throw new IllegalStateException(
                    "Datadog checkpoint version or source settings changed.");
        }
    }

    static byte[] digest(String value) {
        return digest(new byte[0], value);
    }

    static byte[] digest(byte[] prior, String value) {
        try {
            MessageDigest hasher = MessageDigest.getInstance("SHA-256");
            hasher.update(prior);
            return hasher.digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 unavailable");
        }
    }

    private void validateEnd(Instant end) {
        Duration duration = Duration.between(start, end);
        if (duration.isNegative() || duration.compareTo(MAX_RANGE) > 0) {
            throw new IllegalArgumentException(
                    "Datadog time bounds must be ordered and at most 36525 days apart.");
        }
    }

    private static String readKey(ReadonlyConfig options, Option<String> option) {
        String value = options.get(option);
        if (value == null
                || value.isEmpty()
                || value.length() > 256
                || !value.chars().allMatch(c -> c >= 33 && c <= 126)) {
            throw new IllegalArgumentException("Invalid Datadog credential option " + option.key());
        }
        return value;
    }

    private static Instant instant(ReadonlyConfig options, Option<String> option) {
        String value = options.get(option);
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            Instant instant = Instant.parse(value);
            if (instant.isBefore(Instant.parse("0001-01-01T00:00:00Z"))
                    || instant.isAfter(Instant.parse("9999-12-31T00:00:00Z"))) {
                throw new IllegalArgumentException(
                        "Datadog time bound is outside the supported calendar.");
            }
            return instant;
        } catch (DateTimeException ex) {
            throw new IllegalArgumentException(
                    "Invalid Datadog ISO-8601 instant in " + option.key());
        }
    }

    private static int bounded(
            ReadonlyConfig options, Option<Integer> option, int lower, int upper) {
        int value = options.get(option);
        if (value < lower || value > upper) {
            throw new IllegalArgumentException(
                    "Datadog option " + option.key() + " is outside the supported range.");
        }
        return value;
    }

    private static String escape(String query) {
        return new String(JsonStringEncoder.getInstance().quoteAsString(query));
    }
}

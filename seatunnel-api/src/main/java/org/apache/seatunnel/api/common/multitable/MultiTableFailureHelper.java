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

package org.apache.seatunnel.api.common.multitable;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.MultiTableCommonOptions;
import org.apache.seatunnel.api.options.MultiTableFailurePolicy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class MultiTableFailureHelper {

    public static final String ISOLATED_FAILURE_MARKER = "[MULTI_TABLE_ISOLATED_FAILURE]";

    private static final String FAILED_TABLE_DELIMITER = ":";
    private static final int FAILED_TABLE_FIELD_COUNT = 6;
    private static final ThreadLocal<List<MultiTableFailedTable>> FAILED_TABLE_COLLECTOR =
            new ThreadLocal<>();

    private MultiTableFailureHelper() {}

    public static boolean shouldContinueOtherTables(ReadonlyConfig options) {
        return options != null
                && options.get(MultiTableCommonOptions.MULTI_TABLE_FAILURE_POLICY)
                        .continueOtherTables();
    }

    public static ReadonlyConfig mergeOptions(ReadonlyConfig primary, ReadonlyConfig fallback) {
        if (primary == null) {
            return fallback;
        }
        if (fallback == null) {
            return primary;
        }
        return ReadonlyConfig.fromConfig(primary.toConfig().withFallback(fallback.toConfig()));
    }

    public static ReadonlyConfig withMultiTableFailurePolicy(
            ReadonlyConfig options, ReadonlyConfig envOptions) {
        if (envOptions == null) {
            return options;
        }
        Map<String, Object> internalOptions = new HashMap<>();
        MultiTableFailurePolicy failurePolicy =
                envOptions.get(MultiTableCommonOptions.MULTI_TABLE_FAILURE_POLICY);
        internalOptions.put(
                MultiTableCommonOptions.MULTI_TABLE_FAILURE_POLICY.key(), failurePolicy.name());
        return mergeOptions(ReadonlyConfig.fromMap(internalOptions), options);
    }

    public static ReadonlyConfig withFailedTables(
            ReadonlyConfig options, Collection<MultiTableFailedTable> failedTables) {
        if (failedTables == null || failedTables.isEmpty()) {
            return options;
        }
        Map<String, Object> internalOptions = new HashMap<>();
        internalOptions.put(
                MultiTableCommonOptions.MULTI_TABLE_INITIAL_FAILED_TABLES.key(),
                serializeFailedTables(failedTables));
        return mergeOptions(ReadonlyConfig.fromMap(internalOptions), options);
    }

    public static MultiTableFailedTable buildFailedTable(
            String tablePath, MultiTableFailurePhase phase, String pluginName, Throwable error) {
        String message = summarizeThrowable(error);
        return new MultiTableFailedTable(
                tablePath,
                phase,
                pluginName,
                error == null ? "UnknownException" : error.getClass().getSimpleName(),
                message,
                System.currentTimeMillis(),
                error);
    }

    public static <T> T collectFailedTables(
            Collection<MultiTableFailedTable> target, Supplier<T> action) {
        List<MultiTableFailedTable> previous = FAILED_TABLE_COLLECTOR.get();
        List<MultiTableFailedTable> current = new ArrayList<>();
        FAILED_TABLE_COLLECTOR.set(current);
        try {
            return action.get();
        } finally {
            if (target != null) {
                target.addAll(current);
            }
            if (previous == null) {
                FAILED_TABLE_COLLECTOR.remove();
            } else {
                previous.addAll(current);
                FAILED_TABLE_COLLECTOR.set(previous);
            }
        }
    }

    public static void recordFailedTable(MultiTableFailedTable failedTable) {
        List<MultiTableFailedTable> failedTables = FAILED_TABLE_COLLECTOR.get();
        if (failedTables != null && failedTable != null) {
            failedTables.add(failedTable);
        }
    }

    public static List<MultiTableFailedTable> getInitialFailedTables(ReadonlyConfig options) {
        if (options == null) {
            return Collections.emptyList();
        }
        return options.getOptional(MultiTableCommonOptions.MULTI_TABLE_INITIAL_FAILED_TABLES)
                .orElse(Collections.emptyList()).stream()
                .map(MultiTableFailureHelper::deserializeFailedTable)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public static String formatFailedTableSummary(
            String title, Collection<MultiTableFailedTable> failedTables) {
        if (failedTables == null || failedTables.isEmpty()) {
            return title;
        }
        Map<String, MultiTableFailedTable> sortedFailures = new TreeMap<>();
        failedTables.forEach(failure -> sortedFailures.put(failure.getTablePath(), failure));
        return title
                + System.lineSeparator()
                + sortedFailures.values().stream()
                        .map(MultiTableFailureHelper::formatFailedTableLine)
                        .collect(Collectors.joining(System.lineSeparator()));
    }

    public static String withIsolatedFailureMarker(String message) {
        if (message == null || message.isEmpty()) {
            return ISOLATED_FAILURE_MARKER;
        }
        if (isIsolatedFailure(message)) {
            return message;
        }
        return ISOLATED_FAILURE_MARKER + System.lineSeparator() + message;
    }

    public static boolean isIsolatedFailure(String message) {
        return message != null && message.contains(ISOLATED_FAILURE_MARKER);
    }

    public static String formatFailedTableLine(MultiTableFailedTable failure) {
        return String.format(
                "table=%s, phase=%s, plugin=%s, exception=%s, reason=%s",
                failure.getTablePath(),
                failure.getPhase(),
                failure.getPluginName(),
                failure.getExceptionClass(),
                failure.getMessageSummary());
    }

    public static String summarizeThrowable(Throwable error) {
        Throwable current = error;
        String message = null;
        while (current != null) {
            if (current.getMessage() != null && !current.getMessage().trim().isEmpty()) {
                message = current.getMessage().trim();
            }
            current = current.getCause();
        }
        if (message == null || message.isEmpty()) {
            message = error == null ? "" : error.getClass().getName();
        }
        message = message.replaceAll("\\s+", " ");
        if (message.length() > 300) {
            return message.substring(0, 297) + "...";
        }
        return message;
    }

    private static List<String> serializeFailedTables(
            Collection<MultiTableFailedTable> failedTables) {
        return failedTables.stream()
                .map(MultiTableFailureHelper::serializeFailedTable)
                .collect(Collectors.toList());
    }

    private static String serializeFailedTable(MultiTableFailedTable failedTable) {
        return Stream.of(
                        failedTable.getTablePath(),
                        failedTable.getPhase().name(),
                        failedTable.getPluginName(),
                        failedTable.getExceptionClass(),
                        failedTable.getMessageSummary(),
                        String.valueOf(failedTable.getFirstFailureTime()))
                .map(MultiTableFailureHelper::encodeField)
                .collect(Collectors.joining(FAILED_TABLE_DELIMITER));
    }

    private static MultiTableFailedTable deserializeFailedTable(String serialized) {
        try {
            String[] parts = serialized.split(FAILED_TABLE_DELIMITER, -1);
            if (parts.length != FAILED_TABLE_FIELD_COUNT) {
                return null;
            }
            return new MultiTableFailedTable(
                    decodeField(parts[0]),
                    MultiTableFailurePhase.valueOf(decodeField(parts[1])),
                    decodeField(parts[2]),
                    decodeField(parts[3]),
                    decodeField(parts[4]),
                    Long.parseLong(decodeField(parts[5])),
                    null);
        } catch (RuntimeException ignore) {
            return null;
        }
    }

    private static String encodeField(String value) {
        return Base64.getUrlEncoder()
                .encodeToString((value == null ? "" : value).getBytes(StandardCharsets.UTF_8));
    }

    private static String decodeField(String value) {
        return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
    }

    public static boolean isFailFast(ReadonlyConfig options) {
        return options == null
                || options.get(MultiTableCommonOptions.MULTI_TABLE_FAILURE_POLICY)
                        == MultiTableFailurePolicy.FAIL_FAST;
    }
}

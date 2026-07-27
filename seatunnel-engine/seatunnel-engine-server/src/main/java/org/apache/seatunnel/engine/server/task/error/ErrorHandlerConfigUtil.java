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

package org.apache.seatunnel.engine.server.task.error;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class ErrorHandlerConfigUtil {

    private static final int DEFAULT_MAX_ERROR_RATIO_MIN_RECORDS = 10000;

    public enum StageType {
        TRANSFORM,
        SINK
    }

    public static StageErrorConfig buildStageConfig(
            Map<String, Object> envOptions, StageType stageType) {
        return buildStageConfig(envOptions, stageType, -1L);
    }

    public static StageErrorConfig buildStageConfig(
            Map<String, Object> envOptions, StageType stageType, long jobId) {
        if (envOptions == null || envOptions.isEmpty()) {
            return disabledConfig();
        }

        ReadonlyConfig envConfig = ReadonlyConfig.fromMap(envOptions);
        Map<String, Object> root = envConfig.getSourceMap();

        Map<String, Object> global =
                getNestedMap(root, "error_handler").orElse(Collections.emptyMap());

        String stageKey =
                stageType == StageType.TRANSFORM ? "transform_error_handler" : "sink_error_handler";
        Map<String, Object> stage = getNestedMap(root, stageKey).orElse(Collections.emptyMap());

        String modeStr = getString(stage, global, "mode", "DISABLE");
        ErrorHandlerMode mode = ErrorHandlerMode.fromString(modeStr);

        double maxErrorRatio = getDouble(stage, global, "max_error_ratio", 0.0d);
        if (maxErrorRatio < 0.0d || maxErrorRatio > 1.0d) {
            throw new IllegalArgumentException(
                    "error handler max_error_ratio must be between 0.0 and 1.0, but was "
                            + maxErrorRatio);
        }
        int maxErrorRatioMinRecords =
                getNonNegativeInt(
                        stage,
                        global,
                        "max_error_ratio_min_records",
                        DEFAULT_MAX_ERROR_RATIO_MIN_RECORDS);
        long maxErrorRecords = getLong(stage, global, "max_error_records", 0L);
        if (maxErrorRecords < 0L) {
            throw new IllegalArgumentException(
                    "error handler max_error_records must be non-negative, but was "
                            + maxErrorRecords);
        }

        int queueCapacity = getNonNegativeInt(stage, global, "queue_capacity", 10000);
        String overflowStr = getString(stage, global, "queue_overflow_policy", "FAIL");
        QueueOverflowPolicy overflowPolicy = QueueOverflowPolicy.fromString(overflowStr);

        boolean includeStacktrace = getBoolean(stage, global, "include_stacktrace", false);
        boolean includeOriginalData = getBoolean(stage, global, "include_original_data", false);

        String dataFormatStr = getString(stage, global, "original_data_format", "TEXT");
        if (!"TEXT".equalsIgnoreCase(dataFormatStr)) {
            throw new IllegalArgumentException(
                    "Unsupported original_data_format='"
                            + dataFormatStr
                            + "'. Current version only supports TEXT.");
        }
        OriginalDataFormat originalDataFormat = OriginalDataFormat.TEXT;

        int originalDataMaxLength =
                getNonNegativeInt(stage, global, "original_data_max_length", 8192);

        ErrorSinkConfig sinkConfig = buildErrorSinkConfig(stage, global);

        if (mode == ErrorHandlerMode.ROUTE && (sinkConfig == null || !sinkConfig.isConfigured())) {
            throw new IllegalArgumentException(
                    String.format(
                            "env.%s.mode=ROUTE requires env.%s.sink.plugin_name to be configured.",
                            stageKey, stageKey));
        }

        return StageErrorConfig.builder()
                .mode(mode)
                .sink(sinkConfig)
                .maxErrorRatio(maxErrorRatio)
                .maxErrorRatioMinRecords(maxErrorRatioMinRecords)
                .maxErrorRecords(maxErrorRecords)
                .queueCapacity(queueCapacity)
                .queueOverflowPolicy(overflowPolicy)
                .includeStacktrace(includeStacktrace)
                .includeOriginalData(includeOriginalData)
                .originalDataFormat(originalDataFormat)
                .originalDataMaxLength(originalDataMaxLength)
                .build();
    }

    private static StageErrorConfig disabledConfig() {
        return StageErrorConfig.builder()
                .mode(ErrorHandlerMode.DISABLE)
                .sink(ErrorSinkConfig.empty())
                .maxErrorRatio(0.0d)
                .maxErrorRatioMinRecords(0)
                .maxErrorRecords(0L)
                .queueCapacity(0)
                .queueOverflowPolicy(QueueOverflowPolicy.FAIL)
                .includeStacktrace(false)
                .includeOriginalData(false)
                .originalDataFormat(OriginalDataFormat.TEXT)
                .originalDataMaxLength(0)
                .build();
    }

    @SuppressWarnings("unchecked")
    private static Optional<Map<String, Object>> getNestedMap(
            Map<String, Object> root, String key) {
        Object value = root.get(key);
        if (value instanceof Map) {
            return Optional.of((Map<String, Object>) value);
        }
        return Optional.empty();
    }

    @SuppressWarnings("unchecked")
    private static ErrorSinkConfig buildErrorSinkConfig(
            Map<String, Object> stage, Map<String, Object> global) {
        Map<String, Object> stageSink = getNestedMap(stage, "sink").orElse(Collections.emptyMap());
        Map<String, Object> globalSink =
                getNestedMap(global, "sink").orElse(Collections.emptyMap());

        Map<String, Object> effectiveSink = new HashMap<>(globalSink);
        effectiveSink.putAll(stageSink);
        if (effectiveSink.isEmpty()) {
            return ErrorSinkConfig.empty();
        }

        String pluginName =
                Optional.ofNullable(effectiveSink.get("plugin_name"))
                        .map(Object::toString)
                        .orElse(null);
        String errorTable =
                Optional.ofNullable(effectiveSink.get("error_table"))
                        .map(Object::toString)
                        .orElse(null);

        return new ErrorSinkConfig(pluginName, errorTable, effectiveSink);
    }

    private static String getString(
            Map<String, Object> stage,
            Map<String, Object> global,
            String key,
            String defaultValue) {
        Object value = stage.getOrDefault(key, global.get(key));
        return value == null ? defaultValue : value.toString();
    }

    private static long getLong(
            Map<String, Object> stage, Map<String, Object> global, String key, long defaultValue) {
        Object value = stage.getOrDefault(key, global.get(key));
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value != null) {
            try {
                return Long.parseLong(value.toString());
            } catch (NumberFormatException ignore) {
                throw new IllegalArgumentException(
                        "Invalid error handler numeric value for '" + key + "': " + value, ignore);
            }
        }
        return defaultValue;
    }

    private static double getDouble(
            Map<String, Object> stage,
            Map<String, Object> global,
            String key,
            double defaultValue) {
        Object value = stage.getOrDefault(key, global.get(key));
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value != null) {
            try {
                return Double.parseDouble(value.toString());
            } catch (NumberFormatException ignore) {
                throw new IllegalArgumentException(
                        "Invalid error handler numeric value for '" + key + "': " + value, ignore);
            }
        }
        return defaultValue;
    }

    private static boolean getBoolean(
            Map<String, Object> stage,
            Map<String, Object> global,
            String key,
            boolean defaultValue) {
        Object value = stage.getOrDefault(key, global.get(key));
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value != null) {
            String booleanValue = value.toString().trim();
            if ("true".equalsIgnoreCase(booleanValue)) {
                return true;
            }
            if ("false".equalsIgnoreCase(booleanValue)) {
                return false;
            }
            throw new IllegalArgumentException(
                    "Invalid error handler boolean value for '" + key + "': " + value);
        }
        return defaultValue;
    }

    private static int getNonNegativeInt(
            Map<String, Object> stage, Map<String, Object> global, String key, int defaultValue) {
        long value = getLong(stage, global, key, defaultValue);
        if (value < 0 || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "error handler "
                            + key
                            + " must be between 0 and "
                            + Integer.MAX_VALUE
                            + ", but was "
                            + value);
        }
        return (int) value;
    }
}

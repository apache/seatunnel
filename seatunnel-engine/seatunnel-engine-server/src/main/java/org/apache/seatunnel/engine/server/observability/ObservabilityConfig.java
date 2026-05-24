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

package org.apache.seatunnel.engine.server.observability;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Immutable job-level settings for Zeta realtime observability.
 *
 * <p>The same env options are consumed by planning, worker enablement, and master-side aggregation,
 * so this class is the single place that keeps enablement semantics aligned. Observability remains
 * disabled by default unless users explicitly enable it or configure split points that require the
 * feature to be active.
 */
@Getter
@Slf4j
public class ObservabilityConfig {

    private static final String PREFIX = "engine.observability.";
    private static final int DEFAULT_RETENTION_MINUTES = 3;
    private static final int MAX_RETENTION_MINUTES = 10;
    private static final int MAX_EDGE_OVERRIDE_CAPACITY = 100_000;
    private static final Set<String> LOGGED_EDGE_OVERRIDE_ISSUES = ConcurrentHashMap.newKeySet();

    private final boolean enabled;
    private final long bucketMs;
    private final int retentionMinutes;
    private final List<String> asyncBoundaries;
    /** Default capacity for async boundary queues, 0 means use engine default. */
    private final int edgeBufferCapacity;

    private final Map<String, Integer> edgeOverrides;

    /**
     * Whether to split sink IO into a dedicated queue stage.
     *
     * <p>Default is false because inserting an extra queue for every sink may introduce additional
     * latency/memory overhead and can change execution characteristics. Enable it explicitly when
     * you need queue-level backpressure metrics before sinks.
     */
    private final boolean splitSinkIo;

    private final String jobName;
    private final long createTime;

    private ObservabilityConfig(
            boolean enabled,
            long bucketMs,
            int retentionMinutes,
            List<String> asyncBoundaries,
            int edgeBufferCapacity,
            Map<String, Integer> edgeOverrides,
            boolean splitSinkIo,
            String jobName,
            long createTime) {
        this.enabled = enabled;
        this.bucketMs = bucketMs;
        this.retentionMinutes = retentionMinutes;
        this.asyncBoundaries = asyncBoundaries;
        this.edgeBufferCapacity = edgeBufferCapacity;
        this.edgeOverrides = edgeOverrides;
        this.splitSinkIo = splitSinkIo;
        this.jobName = jobName;
        this.createTime = createTime;
    }

    public static ObservabilityConfig fromEnvOptions(Map<String, Object> envOptions) {
        return fromEnvOptions(envOptions, "Unknown", System.currentTimeMillis());
    }

    public static ObservabilityConfig fromEnvOptions(
            Map<String, Object> envOptions, String jobName, long createTime) {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(envOptions == null ? Collections.emptyMap() : envOptions);

        Boolean enabledExplicit = getEnabledOrNull(envOptions);
        boolean enabled = enabledExplicit != null ? enabledExplicit : false;
        long bucketMs = getLong(config, PREFIX + "bucket_ms", 5000L);
        int retentionMinutesRaw =
                (int) getLong(config, PREFIX + "retention_minutes", DEFAULT_RETENTION_MINUTES);
        int retentionMinutes = Math.min(MAX_RETENTION_MINUTES, Math.max(1, retentionMinutesRaw));
        List<String> asyncBoundaries = getStringList(config, PREFIX + "async_boundaries");
        int edgeBufferCapacity = (int) getLong(config, PREFIX + "edge_buffer_capacity", 0L);
        Map<String, Integer> edgeOverrides = getEdgeOverrides(config, PREFIX + "edge_overrides");
        boolean splitSinkIo = getBoolean(config, PREFIX + "split_sink_io", false);

        // If users configured async boundaries or sink splitting but didn't set `enabled=true`,
        // turn it on automatically to make the config take effect. Respect explicit
        // `enabled=false`.
        boolean hasSplitConfig =
                splitSinkIo || (asyncBoundaries != null && !asyncBoundaries.isEmpty());
        if (!enabled && hasSplitConfig) {
            if (enabledExplicit == null) {
                enabled = true;
            } else {
                log.warn(
                        "Observability is explicitly disabled (engine.observability.enabled=false) "
                                + "but async boundaries or split sink IO is configured. "
                                + "The split config will be ignored. jobName={}, asyncBoundaries={}, splitSinkIo={}",
                        jobName,
                        asyncBoundaries == null ? 0 : asyncBoundaries.size(),
                        splitSinkIo);
            }
        }

        return new ObservabilityConfig(
                enabled,
                Math.max(1000L, bucketMs),
                retentionMinutes,
                asyncBoundaries,
                Math.max(0, edgeBufferCapacity),
                edgeOverrides,
                splitSinkIo,
                jobName,
                createTime);
    }

    public static ObservabilityConfig disabled(String jobName, long createTime) {
        return new ObservabilityConfig(
                false,
                5000L,
                DEFAULT_RETENTION_MINUTES,
                Collections.emptyList(),
                0,
                Collections.emptyMap(),
                false,
                jobName,
                createTime);
    }

    /**
     * Returns the raw configured value of {@code engine.observability.enabled}, or {@code null} if
     * the key is absent.
     *
     * <p>This is used to distinguish "explicitly disabled" from "not configured" (where some
     * callers may want to enable lightweight collection by default).
     */
    public static Boolean getEnabledOrNull(Map<String, Object> envOptions) {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(envOptions == null ? Collections.emptyMap() : envOptions);
        Object value = getValue(config, PREFIX + "enabled");
        if (value == null) {
            return null;
        }
        return Boolean.parseBoolean(String.valueOf(value));
    }

    /**
     * Resolve whether observability should be enabled for this job.
     *
     * <p>This is a lightweight resolver used by workers to avoid expensive parsing and to avoid
     * emitting warning logs per task. The full {@link #fromEnvOptions(Map, String, long)} method is
     * still used on the master for richer config parsing/validation.
     */
    public static boolean resolveEnabled(Map<String, Object> envOptions) {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(envOptions == null ? Collections.emptyMap() : envOptions);

        Boolean enabledExplicit = getEnabledOrNull(envOptions);
        boolean enabled = enabledExplicit != null ? enabledExplicit : false;

        if (enabledExplicit != null) {
            return enabled;
        }

        boolean splitSinkIo = getBoolean(config, PREFIX + "split_sink_io", false);
        List<String> asyncBoundaries = getStringList(config, PREFIX + "async_boundaries");
        boolean hasSplitConfig =
                splitSinkIo || (asyncBoundaries != null && !asyncBoundaries.isEmpty());
        return enabled || hasSplitConfig;
    }

    public ObservabilityConfig withEnabled(boolean enabled) {
        if (this.enabled == enabled) {
            return this;
        }
        return new ObservabilityConfig(
                enabled,
                bucketMs,
                retentionMinutes,
                asyncBoundaries,
                edgeBufferCapacity,
                edgeOverrides,
                splitSinkIo,
                jobName,
                createTime);
    }

    public int capacityForBoundary(String boundaryName) {
        if (boundaryName == null) {
            return edgeBufferCapacity;
        }
        Integer override = edgeOverrides.get(boundaryName);
        return override != null ? override : edgeBufferCapacity;
    }

    @SuppressWarnings("unchecked")
    private static List<String> getStringList(ReadonlyConfig config, String key) {
        Object value = getValue(config, key);
        if (value == null) {
            return Collections.emptyList();
        }
        if (value instanceof List) {
            return ((List<?>) value).stream().map(String::valueOf).collect(Collectors.toList());
        }
        return Collections.singletonList(String.valueOf(value));
    }

    private static boolean getBoolean(ReadonlyConfig config, String key, boolean defaultValue) {
        Object value = getValue(config, key);
        return value == null ? defaultValue : Boolean.parseBoolean(String.valueOf(value));
    }

    private static long getLong(ReadonlyConfig config, String key, long defaultValue) {
        Object value = getValue(config, key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (Exception ignored) {
            return defaultValue;
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Integer> getEdgeOverrides(ReadonlyConfig config, String key) {
        Object value = getValue(config, key);
        if (!(value instanceof List)) {
            return Collections.emptyMap();
        }
        Map<String, Integer> overrides = new HashMap<>();
        for (Object item : (List<?>) value) {
            if (!(item instanceof Map)) {
                warnOnce(
                        "edge_overrides:not_map:"
                                + (item == null ? "null" : item.getClass().getName()),
                        "Invalid {} item (expected map): {}",
                        key,
                        item == null ? "null" : item.getClass().getName());
                continue;
            }
            Map<String, Object> map = (Map<String, Object>) item;
            Object boundary = map.get("boundary");
            Object capacity = map.get("capacity");
            if (boundary == null || capacity == null) {
                warnOnce(
                        "edge_overrides:missing:" + map.keySet(),
                        "Invalid {} item (missing boundary/capacity), keys={}",
                        key,
                        map.keySet());
                continue;
            }
            try {
                int cap = Integer.parseInt(String.valueOf(capacity));
                if (cap < 0) {
                    warnOnce(
                            "edge_overrides:negative:" + boundary + ":" + cap,
                            "Invalid {} item (capacity must be >= 0), boundary={}, capacity={}",
                            key,
                            boundary,
                            cap);
                    continue;
                }
                if (cap > MAX_EDGE_OVERRIDE_CAPACITY) {
                    warnOnce(
                            "edge_overrides:too_large:" + boundary + ":" + cap,
                            "Invalid {} item (capacity must be <= {}), boundary={}, capacity={}. Clamp to {}.",
                            key,
                            MAX_EDGE_OVERRIDE_CAPACITY,
                            boundary,
                            cap,
                            MAX_EDGE_OVERRIDE_CAPACITY);
                    cap = MAX_EDGE_OVERRIDE_CAPACITY;
                }
                overrides.put(String.valueOf(boundary), cap);
            } catch (Exception e) {
                warnOnce(
                        "edge_overrides:invalid:" + boundary + ":" + capacity,
                        "Invalid {} item (cannot parse capacity), boundary={}, capacity={}",
                        key,
                        boundary,
                        capacity);
            }
        }
        return overrides;
    }

    private static void warnOnce(String uniqKey, String message, Object... args) {
        if (!LOGGED_EDGE_OVERRIDE_ISSUES.add(uniqKey)) {
            return;
        }
        log.warn(message, args);
    }

    @SuppressWarnings("unchecked")
    private static Object getValue(ReadonlyConfig config, String key) {
        if (config == null) {
            return null;
        }
        Map<String, Object> data = config.getSourceMap();
        if (data == null) {
            return null;
        }
        if (data.containsKey(key)) {
            return data.get(key);
        }
        String[] keys = key.split("\\.");
        Object value = null;
        for (int i = 0; i < keys.length; i++) {
            value = data.get(keys[i]);
            if (i < keys.length - 1) {
                if (!(value instanceof Map)) {
                    return null;
                }
                data = (Map<String, Object>) value;
            }
        }
        return value;
    }
}

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

package org.apache.seatunnel.lineage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Immutable lineage configuration with per-option source precedence. */
public final class LineageConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String ENABLED = "openlineage_enabled";
    public static final String TRANSPORT = "openlineage_transport";
    public static final String URL = "openlineage_url";
    public static final String NAMESPACE = "openlineage_namespace";
    public static final String AUTH_TOKEN = "openlineage_auth_token";
    public static final String TIMEOUT_MS = "openlineage_timeout_ms";
    public static final String RETRY_TIMES = "openlineage_retry_times";
    public static final String RUN_FACET = "openlineage_run_facet";
    public static final String RUN_PROPERTIES = "openlineage_run_properties";
    public static final String HEARTBEAT_MIN_INTERVAL_MS = "openlineage_heartbeat_min_interval_ms";
    public static final String PRODUCER = "openlineage_producer";

    public static final String DEFAULT_TRANSPORT = "http";
    public static final String DEFAULT_NAMESPACE = "seatunnel";
    public static final String DEFAULT_RUN_FACET = "seatunnel_properties";
    public static final int DEFAULT_TIMEOUT_MS = 10000;
    public static final int DEFAULT_RETRY_TIMES = 3;
    public static final long DEFAULT_HEARTBEAT_MIN_INTERVAL_MS = 3600000L;

    private final boolean enabled;
    private final String transport;
    private final String url;
    private final String namespace;
    private final String authToken;
    private final int timeoutMs;
    private final int retryTimes;
    private final String runFacet;
    private final Map<String, Object> runProperties;
    private final long heartbeatMinIntervalMs;
    private final String producer;

    private LineageConfig(
            boolean enabled,
            String transport,
            String url,
            String namespace,
            String authToken,
            int timeoutMs,
            int retryTimes,
            String runFacet,
            Map<String, Object> runProperties,
            long heartbeatMinIntervalMs,
            String producer) {
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException(TIMEOUT_MS + " must be greater than zero");
        }
        if (retryTimes < 0) {
            throw new IllegalArgumentException(RETRY_TIMES + " must not be negative");
        }
        if (heartbeatMinIntervalMs < 0) {
            throw new IllegalArgumentException(HEARTBEAT_MIN_INTERVAL_MS + " must not be negative");
        }
        this.enabled = enabled;
        this.transport = LineageValidation.requireText(transport, TRANSPORT);
        this.url = blankToNull(url);
        this.namespace = LineageValidation.requireText(namespace, NAMESPACE);
        this.authToken = blankToNull(authToken);
        this.timeoutMs = timeoutMs;
        this.retryTimes = retryTimes;
        this.runFacet = LineageValidation.requireText(runFacet, RUN_FACET);
        this.runProperties =
                Collections.unmodifiableMap(
                        runProperties == null
                                ? new LinkedHashMap<>()
                                : new LinkedHashMap<>(runProperties));
        this.heartbeatMinIntervalMs = heartbeatMinIntervalMs;
        this.producer = LineageValidation.requireText(producer, PRODUCER);
    }

    /** Returns the default configuration with lineage reporting disabled. */
    public static LineageConfig defaults() {
        return new LineageConfig(
                false,
                DEFAULT_TRANSPORT,
                null,
                DEFAULT_NAMESPACE,
                null,
                DEFAULT_TIMEOUT_MS,
                DEFAULT_RETRY_TIMES,
                DEFAULT_RUN_FACET,
                Collections.emptyMap(),
                DEFAULT_HEARTBEAT_MIN_INTERVAL_MS,
                defaultProducer());
    }

    /**
     * Resolves each option independently using job env, process env, cluster config, then defaults.
     * The token deliberately skips job env and is rejected there instead. String map values accept
     * the Flink forms {@code key:value,key2:value2} and {@code {key: value, key2: value2}}; an
     * equals sign is also accepted for compatibility with common environment-variable syntax.
     */
    public static LineageConfig resolve(
            Map<String, ?> jobOptions, Map<String, ?> clusterOptions, Map<String, ?> environment) {
        rejectJobAuthToken(jobOptions);
        Map<String, ?> job = nonNull(jobOptions);
        Map<String, ?> cluster = nonNull(clusterOptions);
        Map<String, ?> env = nonNull(environment);
        return new LineageConfig(
                asBoolean(first(job, env, cluster, ENABLED), false),
                asString(first(job, env, cluster, TRANSPORT), TRANSPORT, DEFAULT_TRANSPORT),
                asNullableString(first(job, env, cluster, URL)),
                asString(first(job, env, cluster, NAMESPACE), NAMESPACE, DEFAULT_NAMESPACE),
                asToken(env, cluster),
                asInt(first(job, env, cluster, TIMEOUT_MS), TIMEOUT_MS, DEFAULT_TIMEOUT_MS),
                asInt(first(job, env, cluster, RETRY_TIMES), RETRY_TIMES, DEFAULT_RETRY_TIMES),
                asString(first(job, env, cluster, RUN_FACET), RUN_FACET, DEFAULT_RUN_FACET),
                resolveRunProperties(job, env, cluster),
                asLong(
                        first(job, env, cluster, HEARTBEAT_MIN_INTERVAL_MS),
                        HEARTBEAT_MIN_INTERVAL_MS,
                        DEFAULT_HEARTBEAT_MIN_INTERVAL_MS),
                asString(first(job, env, cluster, PRODUCER), PRODUCER, defaultProducer()));
    }

    /**
     * Resolves only the enabled flag, using the same precedence and key aliases as {@link
     * #resolve}.
     *
     * <p>Callers on the submission path need this before a full resolution is possible, and must
     * not answer it with their own lookup: a second implementation drifts from the alias rules here
     * and then disagrees with the configuration the job actually runs with.
     */
    public static boolean isEnabled(
            Map<String, ?> jobOptions, Map<String, ?> clusterOptions, Map<String, ?> environment) {
        return asBoolean(first(jobOptions, environment, clusterOptions, ENABLED), false);
    }

    /** Rejects a token in job options because job configuration may be persisted or serialized. */
    public static void rejectJobAuthToken(Map<String, ?> jobOptions) {
        if (contains(jobOptions, AUTH_TOKEN)) {
            throw new IllegalArgumentException(
                    AUTH_TOKEN
                            + " is not allowed in job env; use an environment or cluster setting");
        }
    }

    /** Resolves a token from process environment first and cluster configuration second. */
    public static String resolveToken(Map<String, ?> clusterOptions, Map<String, ?> environment) {
        return asToken(nonNull(environment), nonNull(clusterOptions));
    }

    /** Returns whether lineage reporting is enabled. */
    public boolean enabled() {
        return enabled;
    }

    /** Returns the selected lineage transport name. */
    public String transport() {
        return transport;
    }

    /** Returns the lineage receiver URL, or {@code null} when it is not configured. */
    public String url() {
        return url;
    }

    /** Returns the OpenLineage job namespace. */
    public String namespace() {
        return namespace;
    }

    /** Returns the resolved authentication token, if any. */
    public String authToken() {
        return authToken;
    }

    /** Returns the per-attempt send timeout in milliseconds. */
    public int timeoutMs() {
        return timeoutMs;
    }

    /** Returns the number of retries after the initial send attempt. */
    public int retryTimes() {
        return retryTimes;
    }

    /** Returns the run facet name used for custom properties. */
    public String runFacet() {
        return runFacet;
    }

    /** Returns immutable custom properties for the run facet. */
    public Map<String, Object> runProperties() {
        return runProperties;
    }

    /** Returns the minimum interval between streaming heartbeat events. */
    public long heartbeatMinIntervalMs() {
        return heartbeatMinIntervalMs;
    }

    /** Returns the producer URI placed in emitted OpenLineage events. */
    public String producer() {
        return producer;
    }

    /** Returns non-secret values suitable for a serialized callback. */
    public Map<String, Object> toNonSensitiveMap() {
        Map<String, Object> result = toMap();
        result.remove(AUTH_TOKEN);
        return result;
    }

    /** Returns a copy with a token resolved only on the execution side. */
    public LineageConfig withAuthToken(String token) {
        return new LineageConfig(
                enabled,
                transport,
                url,
                namespace,
                token,
                timeoutMs,
                retryTimes,
                runFacet,
                runProperties,
                heartbeatMinIntervalMs,
                producer);
    }

    /** Returns the option map used by the contract tests and serialized callbacks. */
    public Map<String, Object> toMap() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put(ENABLED, enabled);
        result.put(TRANSPORT, transport);
        if (url != null) {
            result.put(URL, url);
        }
        result.put(NAMESPACE, namespace);
        if (authToken != null) {
            result.put(AUTH_TOKEN, authToken);
        }
        result.put(TIMEOUT_MS, timeoutMs);
        result.put(RETRY_TIMES, retryTimes);
        result.put(RUN_FACET, runFacet);
        if (!runProperties.isEmpty()) {
            result.put(RUN_PROPERTIES, runProperties);
        }
        result.put(HEARTBEAT_MIN_INTERVAL_MS, heartbeatMinIntervalMs);
        result.put(PRODUCER, producer);
        return result;
    }

    @Override
    public String toString() {
        return "LineageConfig" + toNonSensitiveMap();
    }

    private static String asToken(Map<String, ?> environment, Map<String, ?> cluster) {
        Lookup environmentValue = findEnvironment(environment, AUTH_TOKEN);
        if (environmentValue.present) {
            return asNullableString(environmentValue);
        }
        Lookup clusterValue = find(cluster, AUTH_TOKEN);
        return clusterValue.present ? asNullableString(clusterValue) : null;
    }

    private static Lookup first(
            Map<String, ?> job, Map<String, ?> environment, Map<String, ?> cluster, String key) {
        Lookup lookup = find(job, key);
        if (lookup.present) {
            return lookup;
        }
        lookup = findEnvironment(environment, key);
        if (lookup.present) {
            return lookup;
        }
        lookup = find(cluster, key);
        return lookup.present ? lookup : Lookup.absent();
    }

    private static Lookup find(Map<String, ?> values, String key) {
        if (values == null || values.isEmpty()) {
            return Lookup.absent();
        }
        if (values.containsKey(key)) {
            return Lookup.of(values.get(key));
        }
        String dotted = key.replace('_', '.');
        if (values.containsKey(dotted)) {
            return Lookup.of(values.get(dotted));
        }
        String prefixed = "openlineage." + key.substring("openlineage_".length());
        if (values.containsKey(prefixed)) {
            return Lookup.of(values.get(prefixed));
        }
        Object nested = values.get("openlineage");
        if (nested instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) nested;
            String suffix = key.substring("openlineage_".length());
            if (map.containsKey(suffix)) {
                return Lookup.of(map.get(suffix));
            }
            if (map.containsKey(key)) {
                return Lookup.of(map.get(key));
            }
        }
        return Lookup.absent();
    }

    private static Lookup findEnvironment(Map<String, ?> values, String key) {
        if (values == null || values.isEmpty()) {
            return Lookup.absent();
        }
        String environmentKey = key.toUpperCase(Locale.ROOT);
        return values.containsKey(environmentKey)
                ? Lookup.of(values.get(environmentKey))
                : Lookup.absent();
    }

    private static boolean contains(Map<String, ?> values, String key) {
        return find(values, key).present;
    }

    private static Map<String, ?> nonNull(Map<String, ?> values) {
        return values == null ? Collections.emptyMap() : values;
    }

    private static boolean asBoolean(Lookup value, boolean fallback) {
        return value.present && value.value != null
                ? Boolean.parseBoolean(String.valueOf(value.value))
                : fallback;
    }

    private static int asInt(Lookup value, String key, int fallback) {
        if (!value.present || value.value == null) {
            return fallback;
        }
        String text = String.valueOf(value.value);
        try {
            return Integer.parseInt(text.trim());
        } catch (NumberFormatException error) {
            throw notANumber(key, text);
        }
    }

    private static long asLong(Lookup value, String key, long fallback) {
        if (!value.present || value.value == null) {
            return fallback;
        }
        String text = String.valueOf(value.value);
        try {
            return Long.parseLong(text.trim());
        } catch (NumberFormatException error) {
            throw notANumber(key, text);
        }
    }

    /**
     * Names the option a malformed number came from.
     *
     * <p>The bare {@code NumberFormatException} says only what the text was. Every caller here
     * resolves several numeric options at once, and the engines re-resolve the configuration on
     * every reported event, so an unnamed failure appears repeatedly in a log that never says which
     * setting to correct.
     */
    private static IllegalArgumentException notANumber(String key, String value) {
        return new IllegalArgumentException(key + " must be a number, but was \"" + value + "\"");
    }

    private static String asString(Lookup value, String key, String fallback) {
        return value.present && value.value != null
                ? LineageValidation.requireText(String.valueOf(value.value), key)
                : fallback;
    }

    private static String asNullableString(Lookup value) {
        return value.present && value.value != null
                ? blankToNull(String.valueOf(value.value))
                : null;
    }

    private static Map<String, Object> resolveRunProperties(
            Map<String, ?> job, Map<String, ?> environment, Map<String, ?> cluster) {
        Map<String, Object> properties = runProperties(job, false);
        if (properties == null) {
            properties = runProperties(environment, true);
        }
        if (properties == null) {
            properties = runProperties(cluster, false);
        }
        return properties == null ? new LinkedHashMap<>() : properties;
    }

    /** Returns the properties declared by one source, or null when it declares none. */
    private static Map<String, Object> runProperties(Map<String, ?> values, boolean environment) {
        Lookup direct =
                environment
                        ? findEnvironment(values, RUN_PROPERTIES)
                        : find(values, RUN_PROPERTIES);
        if (direct.present) {
            return asMap(direct);
        }
        Map<String, Object> prefixed = prefixedMap(values, RUN_PROPERTIES, environment);
        return prefixed.isEmpty() ? null : prefixed;
    }

    private static Map<String, Object> asMap(Lookup value) {
        if (!value.present || value.value == null) {
            return new LinkedHashMap<>();
        }
        if (value.value instanceof String) {
            return parseStringMap((String) value.value);
        }
        if (!(value.value instanceof Map)) {
            throw new IllegalArgumentException(
                    RUN_PROPERTIES + " must be a map or a comma-separated string map");
        }
        Map<String, Object> result = new LinkedHashMap<>();
        ((Map<?, ?>) value.value).forEach((key, item) -> result.put(String.valueOf(key), item));
        return result;
    }

    private static Map<String, Object> prefixedMap(
            Map<String, ?> values, String key, boolean environment) {
        Map<String, Object> result = new LinkedHashMap<>();
        if (values == null || values.isEmpty()) {
            return result;
        }
        String[] prefixes =
                environment
                        ? new String[] {key.toUpperCase(Locale.ROOT) + "_"}
                        : new String[] {
                            key + ".", "openlineage.run_properties.", "openlineage.run.properties."
                        };
        for (Map.Entry<String, ?> entry : values.entrySet()) {
            String entryKey = String.valueOf(entry.getKey());
            for (String prefix : prefixes) {
                if (entryKey.startsWith(prefix) && entryKey.length() > prefix.length()) {
                    result.put(entryKey.substring(prefix.length()), entry.getValue());
                    break;
                }
            }
        }
        return result;
    }

    private static Map<String, Object> parseStringMap(String rawValue) {
        String value = rawValue.trim();
        if (value.isEmpty()) {
            return new LinkedHashMap<>();
        }
        if (value.startsWith("{")) {
            if (!value.endsWith("}")) {
                throw malformedMap();
            }
            value = value.substring(1, value.length() - 1).trim();
        }
        if (value.isEmpty()) {
            return new LinkedHashMap<>();
        }

        Map<String, Object> result = new LinkedHashMap<>();
        for (String entry : splitMapEntries(value)) {
            int separator = findMapSeparator(entry);
            if (separator <= 0) {
                throw malformedMap();
            }
            String key = unquote(entry.substring(0, separator).trim());
            String item = unquote(entry.substring(separator + 1).trim());
            if (key.isEmpty()) {
                throw malformedMap();
            }
            result.put(key, item);
        }
        return result;
    }

    private static List<String> splitMapEntries(String value) {
        List<String> entries = new ArrayList<>();
        int start = 0;
        int comma;
        while ((comma = nextUnquoted(value, start, ",")) >= 0) {
            entries.add(value.substring(start, comma).trim());
            start = comma + 1;
        }
        entries.add(value.substring(start).trim());
        return entries;
    }

    private static int findMapSeparator(String value) {
        return nextUnquoted(value, 0, ":=");
    }

    /**
     * Returns the index of the first character of {@code stops} at or after {@code from} that is
     * not inside a quoted section, or -1 when the value ends first.
     *
     * <p>Entry splitting and key/value separation share this scan so that the quoting and escaping
     * rules cannot drift apart between them.
     *
     * @throws IllegalArgumentException when the value ends inside a quote or a trailing escape
     */
    private static int nextUnquoted(String value, int from, String stops) {
        char quote = 0;
        boolean escaped = false;
        for (int i = from; i < value.length(); i++) {
            char current = value.charAt(i);
            if (quote != 0) {
                if (escaped) {
                    escaped = false;
                } else if (current == '\\') {
                    escaped = true;
                } else if (current == quote) {
                    quote = 0;
                }
            } else if (current == '\'' || current == '"') {
                quote = current;
            } else if (stops.indexOf(current) >= 0) {
                return i;
            }
        }
        if (quote != 0 || escaped) {
            throw malformedMap();
        }
        return -1;
    }

    private static String unquote(String value) {
        if (value.length() < 2) {
            return value;
        }
        char first = value.charAt(0);
        if (first != value.charAt(value.length() - 1) || (first != '\'' && first != '"')) {
            return value;
        }
        String unquoted = value.substring(1, value.length() - 1);
        return unquoted.replace("\\" + first, String.valueOf(first));
    }

    private static IllegalArgumentException malformedMap() {
        return new IllegalArgumentException(
                RUN_PROPERTIES + " must use key:value pairs separated by commas");
    }

    /**
     * Builds the default producer URI from the running SeaTunnel version.
     *
     * <p>This is the single source of the producer default. {@code openlineage_producer} is
     * declared without a default value so that no build-time version string is baked into the
     * option, which would go stale on the next release. The receiver does not validate {@code
     * producer}, so a missing version degrades to the bare project URI rather than to a wrong
     * version.
     */
    private static String defaultProducer() {
        String version = LineageConfig.class.getPackage().getImplementationVersion();
        if (version == null || version.trim().isEmpty()) {
            version = System.getProperty("seatunnel.version", "");
        }
        return version.trim().isEmpty()
                ? "https://seatunnel.apache.org/"
                : "https://seatunnel.apache.org/" + version.trim();
    }

    private static String blankToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value;
    }

    private static final class Lookup {
        private final boolean present;
        private final Object value;

        private Lookup(boolean present, Object value) {
            this.present = present;
            this.value = value;
        }

        private static Lookup of(Object value) {
            return new Lookup(true, value);
        }

        private static Lookup absent() {
            return new Lookup(false, null);
        }
    }
}

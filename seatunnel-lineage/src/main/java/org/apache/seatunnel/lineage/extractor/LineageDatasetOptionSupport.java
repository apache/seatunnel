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

package org.apache.seatunnel.lineage.extractor;

import org.apache.seatunnel.lineage.LineageDataset;
import org.apache.seatunnel.lineage.LineageDatasetNaming;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/** Shared option-map helpers for {@link org.apache.seatunnel.lineage.LineageDatasetExtractor}s. */
public final class LineageDatasetOptionSupport {
    private LineageDatasetOptionSupport() {}

    /**
     * Maps every resolvable table entry of a connector block through one naming rule.
     *
     * <p>Entries that resolve to no table reference, or whose naming function returns empty, are
     * omitted from the result.
     */
    public static List<LineageDataset> datasets(
            Map<String, ?> options, Function<TableReference, Optional<LineageDataset>> naming) {
        List<LineageDataset> result = new ArrayList<>();
        for (Map<String, ?> table : tables(options)) {
            TableReference reference = tableReference(table);
            if (reference == null) {
                continue;
            }
            naming.apply(reference).ifPresent(result::add);
        }
        return result;
    }

    /**
     * Resolves the list of per-table option maps from a connector block.
     *
     * <p>When {@code table_list} / {@code table-list} / {@code tables} is a {@link List}, each
     * element that is a {@link Map} is kept and non-Map entries are silently dropped. If that list
     * yields no maps (absent, empty, or only non-maps), the whole {@code options} map is treated as
     * a single table entry. CDC {@code table-names} entries are expanded before that fallback.
     */
    public static List<Map<String, ?>> tables(Map<String, ?> options) {
        Object tableList = first(options, "table_list", "table-list", "tables");
        if (tableList instanceof List) {
            List<Map<String, ?>> result = new ArrayList<>();
            for (Object value : (List<?>) tableList) {
                if (value instanceof Map) {
                    result.add((Map<String, ?>) value);
                }
            }
            if (!result.isEmpty()) {
                return result;
            }
        }
        // CDC table-names are fully qualified; database-names only filters the subscription.
        Object tableNames = options.get("table-names");
        if (tableNames instanceof List) {
            List<Map<String, ?>> result = new ArrayList<>();
            for (Object value : (List<?>) tableNames) {
                if (value != null) {
                    result.add(Collections.singletonMap("table_path", String.valueOf(value)));
                }
            }
            if (!result.isEmpty()) {
                return result;
            }
        }
        return Collections.singletonList(options);
    }

    /**
     * Returns the host of the first comma-separated address in {@code value}, or {@code null} when
     * {@code value} is {@code null}.
     */
    public static String firstHost(String value) {
        if (value == null) {
            return null;
        }
        String first = value.split(",")[0].trim();
        try {
            URI uri = URI.create(first.contains("://") ? first : "http://" + first);
            return uri.getHost() == null ? first.split(":")[0] : uri.getHost();
        } catch (IllegalArgumentException e) {
            return first.split(":")[0];
        }
    }

    /**
     * Parses the first present key as an integer.
     *
     * <p>Returns {@code fallback} when none of the keys is present (or the present value is {@code
     * null}). Returns {@code null} when a key is present but its value cannot be parsed as an
     * integer, so callers can treat an explicit bad value as "no identity" rather than silently
     * substituting the default.
     */
    public static Integer integer(Map<String, ?> options, int fallback, String... keys) {
        Object value = first(options, keys);
        if (value == null) {
            return fallback;
        }
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Returns {@link String#valueOf(Object)} of the first present key, or {@code null} when none of
     * the keys is present (or the present value is {@code null}).
     */
    public static String string(Map<String, ?> options, String... keys) {
        Object value = first(options, keys);
        return value == null ? null : String.valueOf(value);
    }

    /**
     * Returns the value of the first key that {@link Map#containsKey(Object) containsKey}, or
     * {@code null} when none match.
     *
     * <p>Uses {@code containsKey} rather than a null-check on {@code get}, so an explicitly stored
     * {@code null} value stops the search and is returned as {@code null} instead of falling
     * through to a later key.
     */
    public static Object first(Map<String, ?> options, String... keys) {
        for (String key : keys) {
            if (options.containsKey(key)) {
                return options.get(key);
            }
        }
        return null;
    }

    /**
     * Strips leading {@code /} characters from {@code value}.
     *
     * <p>Trailing slashes are left intact. Returns {@code null} when {@code value} is {@code null}.
     */
    public static String trimSlashes(String value) {
        if (value == null) {
            return null;
        }
        String result = value;
        while (result.startsWith("/")) {
            result = result.substring(1);
        }
        return result;
    }

    private static TableReference tableReference(Map<String, ?> options) {
        String configuredPath = string(options, "table_path", "table-path", "tablePath");
        String database = string(options, "database", "database_name");
        String table = string(options, "table", "table_name");
        if (configuredPath != null && !configuredPath.trim().isEmpty()) {
            String tablePath = configuredPath.trim();
            if (LineageDatasetNaming.isDefaultTablePath(tablePath)) {
                return null;
            }
            String[] parts = tablePath.split("\\.", -1);
            if (parts.length > 1) {
                database = parts[0];
                table = join(parts, 1);
            } else {
                table = parts[0];
            }
        }
        return new TableReference(database, table);
    }

    private static String join(String[] parts, int start) {
        StringBuilder result = new StringBuilder();
        for (int i = start; i < parts.length; i++) {
            if (i > start) {
                result.append('.');
            }
            result.append(parts[i]);
        }
        return result.toString();
    }

    /** Database/table identity resolved from one table entry of a connector block. */
    public static final class TableReference {
        private final String database;
        private final String table;

        public TableReference(String database, String table) {
            this.database = database;
            this.table = table;
        }

        public String database() {
            return database;
        }

        public String table() {
            return table;
        }
    }
}

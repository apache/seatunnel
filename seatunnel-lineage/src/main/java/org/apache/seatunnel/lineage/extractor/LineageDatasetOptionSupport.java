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

    /** Maps every resolvable table entry of a connector block through one naming rule. */
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
        return Collections.singletonList(options);
    }

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

    public static String string(Map<String, ?> options, String... keys) {
        Object value = first(options, keys);
        return value == null ? null : String.valueOf(value);
    }

    public static Object first(Map<String, ?> options, String... keys) {
        for (String key : keys) {
            if (options.containsKey(key)) {
                return options.get(key);
            }
        }
        return null;
    }

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

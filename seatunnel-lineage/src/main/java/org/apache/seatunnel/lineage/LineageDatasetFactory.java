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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/** Builds canonical datasets from connector option maps without depending on connector classes. */
public final class LineageDatasetFactory {
    private LineageDatasetFactory() {}

    /**
     * Extracts canonical dataset identities from one connector option map.
     *
     * <p>Only connector options that provide a real table identity are returned. Placeholder {@code
     * default.default.default} paths are excluded.
     *
     * @param options connector options, without the enclosing plugin block
     */
    public static List<LineageDataset> fromConnectorOptions(Map<String, ?> options) {
        return fromConnectorOptions(null, options);
    }

    /**
     * Extracts canonical dataset identities for a connector whose plugin name is already known.
     *
     * <p>The plugin name must be supplied by the caller whenever it has one. SeaTunnel jobs
     * normally declare a connector as a named block ({@code sink { Paimon { ... } }}), and the
     * option map for that block does not contain a {@code plugin_name} entry at all, so inferring
     * the name from the options alone silently yields no lineage for the most common job syntax.
     *
     * @param pluginName connector plugin name, or {@code null} to infer it from the options
     * @param options connector options, without the enclosing plugin block
     */
    public static List<LineageDataset> fromConnectorOptions(
            String pluginName, Map<String, ?> options) {
        if (options == null || options.isEmpty()) {
            return Collections.emptyList();
        }
        if (pluginName == null || pluginName.trim().isEmpty()) {
            pluginName = string(options, "plugin_name", "plugin-name");
        }
        if (pluginName == null) {
            pluginName = nestedPluginName(options);
        }
        if (pluginName == null) {
            return Collections.emptyList();
        }
        if ("paimon".equalsIgnoreCase(pluginName)) {
            return paimon(options);
        }
        if ("doris".equalsIgnoreCase(pluginName)) {
            return doris(options);
        }
        if (isJdbc(pluginName, options)) {
            return jdbc(options);
        }
        return Collections.emptyList();
    }

    private static List<LineageDataset> paimon(Map<String, ?> options) {
        String catalog = string(options, "catalog_name", "catalog-name");
        if (catalog == null || catalog.trim().isEmpty()) {
            return Collections.emptyList();
        }
        return datasets(
                options,
                reference ->
                        LineageDatasetNaming.paimon(catalog, reference.database, reference.table));
    }

    private static List<LineageDataset> doris(Map<String, ?> options) {
        String host = firstHost(string(options, "fenodes", "fe_nodes", "fe-nodes"));
        Integer queryPort = integer(options, 9030, "query-port", "query_port", "query.port");
        if (queryPort == null) {
            return Collections.emptyList();
        }
        return datasets(
                options,
                reference ->
                        LineageDatasetNaming.doris(
                                host, queryPort, reference.database, reference.table));
    }

    private static List<LineageDataset> jdbc(Map<String, ?> options) {
        String url = string(options, "url", "jdbc_url", "jdbc-url");
        if (url == null || !url.startsWith("jdbc:")) {
            return Collections.emptyList();
        }
        try {
            URI uri = URI.create(url.substring("jdbc:".length()));
            String scheme = uri.getScheme();
            String host = uri.getHost();
            int port = uri.getPort();
            String urlDatabase = trimSlashes(uri.getPath());
            if (scheme == null || host == null || port <= 0) {
                return Collections.emptyList();
            }
            return datasets(
                    options,
                    reference ->
                            LineageDatasetNaming.jdbc(
                                    scheme,
                                    host,
                                    port,
                                    reference.database == null ? urlDatabase : reference.database,
                                    reference.table));
        } catch (IllegalArgumentException e) {
            return Collections.emptyList();
        }
    }

    /** Maps every resolvable table entry of a connector block through one naming rule. */
    private static List<LineageDataset> datasets(
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

    private static boolean isJdbc(String pluginName, Map<String, ?> options) {
        return string(options, "url", "jdbc_url", "jdbc-url") != null
                || pluginName.toLowerCase().contains("jdbc")
                || pluginName.equalsIgnoreCase("mysql")
                || pluginName.equalsIgnoreCase("postgresql")
                || pluginName.equalsIgnoreCase("oracle");
    }

    private static List<Map<String, ?>> tables(Map<String, ?> options) {
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

    private static String nestedPluginName(Map<String, ?> options) {
        for (Map.Entry<String, ?> entry : options.entrySet()) {
            if (entry.getValue() instanceof Map) {
                String name =
                        string((Map<String, ?>) entry.getValue(), "plugin_name", "plugin-name");
                if (name != null) {
                    return name;
                }
                if ("paimon".equalsIgnoreCase(entry.getKey())
                        || "doris".equalsIgnoreCase(entry.getKey())) {
                    return entry.getKey();
                }
            }
        }
        return null;
    }

    private static String firstHost(String value) {
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

    private static Integer integer(Map<String, ?> options, int fallback, String... keys) {
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

    private static String string(Map<String, ?> options, String... keys) {
        Object value = first(options, keys);
        return value == null ? null : String.valueOf(value);
    }

    private static Object first(Map<String, ?> options, String... keys) {
        for (String key : keys) {
            if (options.containsKey(key)) {
                return options.get(key);
            }
        }
        return null;
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

    private static String trimSlashes(String value) {
        if (value == null) {
            return null;
        }
        String result = value;
        while (result.startsWith("/")) {
            result = result.substring(1);
        }
        return result;
    }

    private static final class TableReference {
        private final String database;
        private final String table;

        private TableReference(String database, String table) {
            this.database = database;
            this.table = table;
        }
    }
}

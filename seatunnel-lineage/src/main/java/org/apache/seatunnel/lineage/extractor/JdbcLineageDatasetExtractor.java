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
import org.apache.seatunnel.lineage.LineageDatasetExtractor;
import org.apache.seatunnel.lineage.LineageDatasetNaming;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Extracts JDBC dataset identities from connector options. */
public final class JdbcLineageDatasetExtractor implements LineageDatasetExtractor {

    private static final Logger LOGGER =
            Logger.getLogger(JdbcLineageDatasetExtractor.class.getName());

    private static final int MYSQL_PORT = 3306;
    private static final int POSTGRESQL_PORT = 5432;
    private static final int SQLSERVER_PORT = 1433;
    private static final int ORACLE_PORT = 1521;

    @Override
    public int order() {
        return 100;
    }

    @Override
    public boolean supports(String pluginName, Map<String, ?> options) {
        if (pluginName == null) {
            return false;
        }
        return LineageDatasetOptionSupport.string(
                                options, "url", "base-url", "jdbc_url", "jdbc-url")
                        != null
                || pluginName.toLowerCase().contains("jdbc")
                || pluginName.equalsIgnoreCase("mysql")
                || pluginName.equalsIgnoreCase("postgresql")
                || pluginName.equalsIgnoreCase("oracle");
    }

    @Override
    public List<LineageDataset> extract(Map<String, ?> options) {
        String url =
                LineageDatasetOptionSupport.string(
                        options, "url", "base-url", "jdbc_url", "jdbc-url");
        if (url == null || !url.startsWith("jdbc:")) {
            return Collections.emptyList();
        }
        JdbcEndpoint endpoint = parse(url);
        if (endpoint == null) {
            // The raw URL is intentionally absent: userinfo and query can carry a password.
            LOGGER.log(
                    Level.WARNING,
                    "Skipping JDBC lineage dataset; JDBC URL scheme {0} did not yield a host and"
                            + " port",
                    jdbcScheme(url));
            return Collections.emptyList();
        }
        return LineageDatasetOptionSupport.datasets(
                options,
                reference ->
                        LineageDatasetNaming.jdbc(
                                endpoint.scheme,
                                endpoint.host,
                                endpoint.port,
                                reference.database() == null
                                        ? endpoint.database
                                        : reference.database(),
                                reference.table()));
    }

    /**
     * Parses the JDBC URL forms this extractor accepts.
     *
     * <p>Hierarchical URLs ({@code scheme://host[:port][/database]}) go through {@link URI}. Oracle
     * thin URLs and SQL Server semicolon properties are not hierarchical, so they are parsed before
     * that. A missing port uses the scheme's well-known default.
     */
    private static JdbcEndpoint parse(String url) {
        String remainder = url.substring("jdbc:".length()).trim();
        String lower = remainder.toLowerCase(Locale.ROOT);
        try {
            if (lower.startsWith("oracle:thin:@")) {
                return parseOracleThin(remainder.substring("oracle:thin:@".length()));
            }
            return parseHierarchical(remainder);
        } catch (IllegalArgumentException failure) {
            return null;
        }
    }

    /** Parses {@code host:port:sid} and {@code //host:port/service} Oracle thin descriptions. */
    private static JdbcEndpoint parseOracleThin(String spec) {
        if (spec.startsWith("(")) {
            return null;
        }
        if (spec.startsWith("//") || spec.indexOf('/') >= 0) {
            String hierarchical = spec.startsWith("//") ? "oracle:" + spec : "oracle://" + spec;
            return endpointFromUri(URI.create(hierarchical), "oracle", ORACLE_PORT, "");
        }
        String[] parts = spec.split(":", 3);
        if (parts.length == 0 || parts[0].trim().isEmpty()) {
            return null;
        }
        String host = parts[0].trim();
        int port = ORACLE_PORT;
        String database = null;
        if (parts.length >= 2 && !parts[1].trim().isEmpty()) {
            port = Integer.parseInt(parts[1].trim());
        }
        if (parts.length == 3) {
            database = blankToNull(stripQuery(parts[2]));
        }
        if (port <= 0) {
            port = ORACLE_PORT;
        }
        return new JdbcEndpoint("oracle", host, port, database);
    }

    /**
     * Parses a hierarchical JDBC URL. Semicolon properties are removed before {@link URI} sees the
     * text, because {@code jdbc:sqlserver://host:1433;databaseName=db} otherwise hides the host.
     */
    private static JdbcEndpoint parseHierarchical(String remainder) {
        int semi = remainder.indexOf(';');
        String uriText = semi < 0 ? remainder : remainder.substring(0, semi);
        String properties = semi < 0 ? "" : remainder.substring(semi + 1);
        URI uri = URI.create(uriText);
        String database = LineageDatasetOptionSupport.trimSlashes(uri.getPath());
        if (database == null || database.isEmpty()) {
            database = jdbcProperty(properties, "databaseName");
            if (database == null) {
                database = jdbcProperty(properties, "database");
            }
        }
        return endpointFromUri(uri, uri.getScheme(), defaultPort(uri.getScheme()), database);
    }

    private static JdbcEndpoint endpointFromUri(
            URI uri, String scheme, int fallbackPort, String database) {
        if (uri == null || scheme == null || uri.getHost() == null) {
            return null;
        }
        int port = uri.getPort() > 0 ? uri.getPort() : fallbackPort;
        if (port <= 0) {
            return null;
        }
        String pathDatabase = LineageDatasetOptionSupport.trimSlashes(uri.getPath());
        if (database == null || database.isEmpty()) {
            database = pathDatabase;
        }
        return new JdbcEndpoint(scheme, uri.getHost(), port, blankToNull(database));
    }

    /** Well-known ports for schemes {@link #supports} already accepts, plus SQL Server. */
    private static int defaultPort(String scheme) {
        if (scheme == null) {
            return -1;
        }
        switch (scheme.toLowerCase(Locale.ROOT)) {
            case "mysql":
            case "mariadb":
                return MYSQL_PORT;
            case "postgresql":
            case "postgres":
                return POSTGRESQL_PORT;
            case "sqlserver":
                return SQLSERVER_PORT;
            case "oracle":
                return ORACLE_PORT;
            default:
                return -1;
        }
    }

    private static String jdbcProperty(String properties, String name) {
        if (properties == null || properties.isEmpty()) {
            return null;
        }
        for (String entry : properties.split(";")) {
            int separator = entry.indexOf('=');
            if (separator <= 0) {
                continue;
            }
            if (entry.substring(0, separator).trim().equalsIgnoreCase(name)) {
                return blankToNull(entry.substring(separator + 1).trim());
            }
        }
        return null;
    }

    /** Scheme token only, so a skipped URL can be logged without its userinfo or query. */
    private static String jdbcScheme(String url) {
        String remainder = url.substring("jdbc:".length());
        int end = remainder.length();
        for (int i = 0; i < remainder.length(); i++) {
            char current = remainder.charAt(i);
            if (current == ':' || current == ';' || current == '/') {
                end = i;
                break;
            }
        }
        return end == 0 ? "<none>" : remainder.substring(0, end);
    }

    private static String stripQuery(String value) {
        int query = value.indexOf('?');
        return query < 0 ? value.trim() : value.substring(0, query).trim();
    }

    private static String blankToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }

    private static final class JdbcEndpoint {
        private final String scheme;
        private final String host;
        private final int port;
        private final String database;

        private JdbcEndpoint(String scheme, String host, int port, String database) {
            this.scheme = scheme;
            this.host = host;
            this.port = port;
            this.database = database;
        }
    }
}

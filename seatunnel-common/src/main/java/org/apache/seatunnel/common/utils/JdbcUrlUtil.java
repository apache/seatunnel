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

package org.apache.seatunnel.common.utils;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import lombok.Data;

import java.io.Serializable;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class JdbcUrlUtil {
    // Standard network database URL pattern (e.g., MySQL, PostgreSQL, etc.)
    private static final Pattern NETWORK_URL_PATTERN =
            Pattern.compile(
                    "^(?<url>jdbc:.+?//(?<host>.+?):(?<port>\\d+?))(/(?<database>.*?))*(?<suffix>\\?.*)*$");

    // File database URL pattern (e.g., DuckDB, SQLite, etc.)
    private static final Pattern FILE_URL_PATTERN =
            Pattern.compile("^jdbc:(?<dbtype>duckdb|sqlite|h2):(?<filepath>.*?)(?<suffix>\\?.*)*$");

    private JdbcUrlUtil() {}

    public static JdbcUrlUtil.UrlInfo getUrlInfo(String url) {
        // First try network database pattern
        Matcher networkMatcher = NETWORK_URL_PATTERN.matcher(url);
        if (networkMatcher.find()) {
            String urlWithoutDatabase = networkMatcher.group("url");
            String database = networkMatcher.group("database");
            return new JdbcUrlUtil.UrlInfo(
                    url,
                    urlWithoutDatabase,
                    networkMatcher.group("host"),
                    Integer.valueOf(networkMatcher.group("port")),
                    database,
                    networkMatcher.group("suffix"));
        }

        // Then try file database pattern
        Matcher fileMatcher = FILE_URL_PATTERN.matcher(url);
        if (fileMatcher.find()) {
            String dbType = fileMatcher.group("dbtype");
            String filePath = fileMatcher.group("filepath");
            String suffix = fileMatcher.group("suffix");

            // For file databases, use file path as database name
            String urlWithoutDatabase = "jdbc:" + dbType + ":";
            String database = filePath.replaceAll("^/+", ""); // Remove leading slashes

            return new JdbcUrlUtil.UrlInfo(
                    url,
                    urlWithoutDatabase,
                    "localhost", // File databases use localhost
                    0, // File databases don't use ports
                    database,
                    suffix);
        }

        throw new IllegalArgumentException("The jdbc url format is incorrect: " + url);
    }

    @Data
    public static class UrlInfo implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String origin;
        private final String urlWithoutDatabase;
        private final String host;
        private final Integer port;
        private final String suffix;
        private final String defaultDatabase;

        public UrlInfo(
                String origin,
                String urlWithoutDatabase,
                String host,
                Integer port,
                String defaultDatabase,
                String suffix) {
            this.origin = origin;
            this.urlWithoutDatabase = urlWithoutDatabase;
            this.host = host;
            this.port = port;
            this.defaultDatabase = defaultDatabase;
            this.suffix = suffix == null ? "" : suffix;
        }

        public Optional<String> getUrlWithDatabase() {
            // For file databases, return original URL directly
            if (port == 0 && "localhost".equals(host)) {
                return Optional.of(origin);
            }
            return StringUtils.isBlank(defaultDatabase)
                    ? Optional.empty()
                    : Optional.of(urlWithoutDatabase + "/" + defaultDatabase + suffix);
        }

        public Optional<String> getDefaultDatabase() {
            // For file databases, always return present (even if empty for in-memory)
            if (port == 0 && "localhost".equals(host)) {
                return Optional.of(defaultDatabase != null ? defaultDatabase : "");
            }
            return StringUtils.isBlank(defaultDatabase)
                    ? Optional.empty()
                    : Optional.of(defaultDatabase);
        }

        public String getUrlWithDatabase(String database) {
            // For file databases, return basic URL + new database path
            if (port == 0 && "localhost".equals(host)) {
                return urlWithoutDatabase + database + (suffix != null ? suffix : "");
            }
            return urlWithoutDatabase + "/" + database + suffix;
        }
    }
}

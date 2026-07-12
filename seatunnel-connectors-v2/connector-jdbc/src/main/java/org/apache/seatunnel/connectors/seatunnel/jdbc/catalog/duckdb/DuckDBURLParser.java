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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.duckdb;

import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for DuckDB JDBC URLs.
 *
 * <p>DuckDB is an embedded database, so URLs look like {@code jdbc:duckdb:}, {@code
 * jdbc:duckdb:/path/to/file.duckdb} or {@code jdbc:duckdb:memory:?option=value}. This parser
 * extracts the embedded database path (if any) and builds {@link JdbcUrlUtil.UrlInfo} accordingly.
 */
public class DuckDBURLParser {

    private static final Pattern DUCKDB_URL_PATTERN =
            Pattern.compile("^jdbc:duckdb:(?<path>[^?]*?)(?<suffix>\\?.*)?$");

    public static JdbcUrlUtil.UrlInfo parse(String url) {
        Matcher matcher = DUCKDB_URL_PATTERN.matcher(url);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid DuckDB JDBC url: " + url);
        }
        String path = Optional.ofNullable(matcher.group("path")).orElse("");
        String suffix = Optional.ofNullable(matcher.group("suffix")).orElse("");
        return new JdbcUrlUtil.UrlInfo(url, "jdbc:duckdb:", "localhost", 0, path, suffix);
    }
}

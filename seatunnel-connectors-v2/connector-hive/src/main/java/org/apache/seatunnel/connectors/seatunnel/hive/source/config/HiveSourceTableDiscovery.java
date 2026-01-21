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

package org.apache.seatunnel.connectors.seatunnel.hive.source.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveOptions;

import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

@UtilityClass
public class HiveSourceTableDiscovery {

    public static boolean isEnabled(ReadonlyConfig config) {
        return config != null && config.get(HiveOptions.USE_REGEX);
    }

    public static List<TablePath> discoverTablePaths(ReadonlyConfig config, Catalog catalog) {
        if (config == null || catalog == null) {
            return Collections.emptyList();
        }

        if (!config.get(HiveOptions.USE_REGEX)) {
            return Collections.emptyList();
        }

        String patternStr = config.getOptional(HiveOptions.TABLE_NAME).orElse(null);
        if (patternStr == null || patternStr.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "When `use_regex` is enabled, `table_name` must be configured");
        }

        HiveTableNamePattern tableNamePattern = HiveTableNamePattern.parse(patternStr);
        Pattern databasePattern = compilePattern(tableNamePattern.getDatabasePattern(), patternStr);
        Pattern tablePattern = compilePattern(tableNamePattern.getTablePattern(), patternStr);

        List<TablePath> tablePaths = new ArrayList<>();
        String databasePatternStr = tableNamePattern.getDatabasePattern();
        if (isExactDatabaseName(databasePatternStr)) {
            String databaseName = databasePatternStr;
            for (String tableName : catalog.listTables(databaseName)) {
                if (tablePattern.matcher(tableName).matches()) {
                    tablePaths.add(TablePath.of(databaseName, tableName));
                }
            }
        } else {
            for (String databaseName : catalog.listDatabases()) {
                if (!databasePattern.matcher(databaseName).matches()) {
                    continue;
                }
                List<String> tables = catalog.listTables(databaseName);
                for (String tableName : tables) {
                    if (tablePattern.matcher(tableName).matches()) {
                        tablePaths.add(TablePath.of(databaseName, tableName));
                    }
                }
            }
        }

        tablePaths.sort(Comparator.comparing(TablePath::getFullName));
        return tablePaths;
    }

    private static Pattern compilePattern(String pattern, String rawTableName) {
        try {
            return Pattern.compile(pattern);
        } catch (PatternSyntaxException exception) {
            throw new IllegalArgumentException(
                    "Invalid regex pattern in `table_name`: "
                            + rawTableName
                            + ", resolved pattern: "
                            + pattern,
                    exception);
        }
    }

    /**
     * Treat databasePattern as an exact database name only when it doesn't contain obvious regex
     * meta characters.
     */
    private static boolean isExactDatabaseName(String databasePattern) {
        if (databasePattern == null || databasePattern.isEmpty()) {
            return false;
        }
        for (int i = 0; i < databasePattern.length(); i++) {
            char ch = databasePattern.charAt(i);
            if (ch == '\\'
                    || ch == '.'
                    || ch == '*'
                    || ch == '+'
                    || ch == '?'
                    || ch == '|'
                    || ch == '['
                    || ch == ']'
                    || ch == '('
                    || ch == ')'
                    || ch == '{'
                    || ch == '}'
                    || ch == '^'
                    || ch == '$') {
                return false;
            }
        }
        return true;
    }
}

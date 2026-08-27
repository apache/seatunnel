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

import java.util.Optional;

final class HiveTableNamePattern {

    private static final String DOT_PLACEHOLDER = "__$DOT$__";

    private final String databasePattern;
    private final String tablePattern;

    private HiveTableNamePattern(String databasePattern, String tablePattern) {
        this.databasePattern = databasePattern;
        this.tablePattern = tablePattern;
    }

    static HiveTableNamePattern parse(String rawPattern) {
        if (rawPattern == null || rawPattern.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "`table_name` must not be blank when `use_regex` is enabled");
        }

        String processed = rawPattern.trim().replace("\\.", DOT_PLACEHOLDER);
        Optional<Integer> separatorIndex = findTableSeparator(processed);
        if (!separatorIndex.isPresent()) {
            throw new IllegalArgumentException(
                    "Hive `table_name` must use `databasePattern.tablePattern` when `use_regex` is enabled. "
                            + "Invalid `table_name`: "
                            + processed.replace(DOT_PLACEHOLDER, "."));
        }

        int index = separatorIndex.get();
        String databasePattern = processed.substring(0, index).trim();
        String tablePattern = processed.substring(index + 1).trim();

        if (databasePattern.isEmpty() || tablePattern.isEmpty()) {
            throw new IllegalArgumentException(
                    "Hive `table_name` must use `databasePattern.tablePattern` when `use_regex` is enabled. "
                            + "Invalid `table_name`: "
                            + processed.replace(DOT_PLACEHOLDER, "."));
        }

        databasePattern = databasePattern.replace(DOT_PLACEHOLDER, ".");
        tablePattern = tablePattern.replace(DOT_PLACEHOLDER, ".");
        return new HiveTableNamePattern(databasePattern, tablePattern);
    }

    private static Optional<Integer> findTableSeparator(String processedPattern) {
        int firstDot = processedPattern.indexOf('.');
        if (firstDot < 0) {
            return Optional.empty();
        }
        int lastDot = processedPattern.lastIndexOf('.');
        if (firstDot != lastDot) {
            throw new IllegalArgumentException(
                    "Hive does not support schema in `table_name` when `use_regex` is enabled. "
                            + "Please use `databasePattern.tablePattern` (only one unescaped '.') and escape dots in regex as '\\.' "
                            + "(in HOCON string, write '\\\\.' instead). "
                            + "Examples: `db0.\\.*`, `db1.user_table_[0-9]+`, `db[1-2].[app|web]order_\\.*`. "
                            + "Invalid `table_name`: "
                            + processedPattern.replace(DOT_PLACEHOLDER, "."));
        }
        return Optional.of(firstDot);
    }

    String getDatabasePattern() {
        return databasePattern;
    }

    String getTablePattern() {
        return tablePattern;
    }
}

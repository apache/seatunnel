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

package org.apache.seatunnel.connectors.cdc.base.utils;

import java.util.Locale;

/** Helpers for classifying captured DDL statements. */
public final class DdlStatements {

    private DdlStatements() {}

    /**
     * Returns whether {@code ddl} is a table-level {@code TRUNCATE} statement.
     *
     * <p>{@code ALTER TABLE ... TRUNCATE PARTITION} and {@code TRUNCATE TABLE ... PARTITION} are
     * excluded because they are not full-table truncates. Table names that merely contain the
     * substring {@code PARTITION} are still treated as table-level truncates.
     */
    public static boolean isTruncateTable(String ddl) {
        if (ddl == null) {
            return false;
        }
        String normalized = ddl.trim().toUpperCase(Locale.ROOT);
        if (normalized.isEmpty() || !normalized.startsWith("TRUNCATE")) {
            return false;
        }
        String[] tokens = normalized.split("\\s+");
        int index = 1;
        if (index < tokens.length && "TABLE".equals(tokens[index])) {
            index++;
        }
        if (index >= tokens.length) {
            return false;
        }
        String tableToken = stripTrailingSemicolons(tokens[index]);
        if (tableToken.isEmpty()) {
            return false;
        }
        index++;
        if (index >= tokens.length) {
            return true;
        }
        return !stripTrailingSemicolons(tokens[index]).startsWith("PARTITION");
    }

    private static String stripTrailingSemicolons(String token) {
        int end = token.length();
        while (end > 0 && token.charAt(end - 1) == ';') {
            end--;
        }
        return token.substring(0, end);
    }
}

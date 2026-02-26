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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Locale;

public final class JdbcIdentifierUtils {

    private JdbcIdentifierUtils() {}

    public enum IdentifierCaseStrategy {
        CASE_SENSITIVE,
        LOWER_CASE,
        UPPER_CASE,
        CASE_INSENSITIVE
    }

    /**
     * Resolve case handling strategy for unquoted identifiers based on {@link DatabaseMetaData}.
     *
     * <p>Note: JDBC metadata APIs often treat {@code schemaPattern}/{@code tableNamePattern} as
     * patterns (e.g. SQL LIKE), while identifier case sensitivity depends on the database. This
     * method provides a best-effort strategy to compare identifiers returned by JDBC metadata APIs.
     */
    public static IdentifierCaseStrategy identifierCaseStrategy(DatabaseMetaData metadata)
            throws SQLException {
        if (metadata == null) {
            return IdentifierCaseStrategy.CASE_INSENSITIVE;
        }
        if (metadata.supportsMixedCaseIdentifiers()) {
            return IdentifierCaseStrategy.CASE_SENSITIVE;
        }
        if (metadata.storesLowerCaseIdentifiers()) {
            return IdentifierCaseStrategy.LOWER_CASE;
        }
        if (metadata.storesUpperCaseIdentifiers()) {
            return IdentifierCaseStrategy.UPPER_CASE;
        }
        return IdentifierCaseStrategy.CASE_INSENSITIVE;
    }

    public static boolean identifierEquals(
            IdentifierCaseStrategy caseStrategy, String expected, String actual) {
        if (expected == null) {
            return true;
        }
        if (actual == null) {
            return false;
        }
        switch (caseStrategy) {
            case CASE_SENSITIVE:
                return actual.equals(expected);
            case LOWER_CASE:
                return actual.toLowerCase(Locale.ROOT).equals(expected.toLowerCase(Locale.ROOT));
            case UPPER_CASE:
                return actual.toUpperCase(Locale.ROOT).equals(expected.toUpperCase(Locale.ROOT));
            case CASE_INSENSITIVE:
            default:
                return actual.equalsIgnoreCase(expected);
        }
    }
}

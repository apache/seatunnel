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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.condition;

import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class UpdateConditionTest {

    private static final List<String> COLUMNS =
            Arrays.asList("id", "name", "update_time", "version", "Update Time", "say\"hi");

    private static String render(String condition) {
        return UpdateCondition.parse(condition, COLUMNS)
                .toSql(column -> "\"" + column + "\"", "EXCLUDED", "target");
    }

    private static String rejectionMessage(String condition) {
        return Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () -> UpdateCondition.parse(condition, COLUMNS))
                .getMessage();
    }

    @Test
    void testIncomingAndStoredColumnsAreQualifiedAndQuoted() {
        Assertions.assertEquals(
                "EXCLUDED.\"update_time\" > target.\"update_time\"",
                render(":update_time > target.update_time"));
        Assertions.assertEquals(
                "EXCLUDED.\"update_time\" > target.\"update_time\"",
                render(":update_time>target.update_time"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"=", "<>", "<", "<=", ">", ">="})
    void testAllComparisonOperators(String operator) {
        Assertions.assertEquals(
                "EXCLUDED.\"version\" " + operator + " target.\"version\"",
                render(":version " + operator + " target.version"));
    }

    @Test
    void testLogicalOperatorsKeepSqlPrecedence() {
        Assertions.assertEquals(
                "(EXCLUDED.\"version\" > target.\"version\" OR (EXCLUDED.\"version\" = target.\"version\""
                        + " AND EXCLUDED.\"update_time\" > target.\"update_time\"))",
                render(
                        ":version > target.version OR :version = target.version"
                                + " AND :update_time > target.update_time"));
        Assertions.assertEquals(
                "((EXCLUDED.\"version\" > target.\"version\" OR EXCLUDED.\"version\" = target.\"version\")"
                        + " AND EXCLUDED.\"update_time\" > target.\"update_time\")",
                render(
                        "(:version > target.version OR :version = target.version)"
                                + " AND :update_time > target.update_time"));
        Assertions.assertEquals(
                "(NOT (NOT EXCLUDED.\"version\" < target.\"version\"))",
                render("NOT NOT :version < target.version"));
    }

    @Test
    void testNullChecks() {
        Assertions.assertEquals(
                "(target.\"update_time\" IS NULL OR EXCLUDED.\"update_time\" > target.\"update_time\")",
                render("target.update_time IS NULL OR :update_time > target.update_time"));
        Assertions.assertEquals(
                "EXCLUDED.\"update_time\" IS NOT NULL", render(":update_time IS NOT NULL"));
    }

    @Test
    void testKeywordsAndTargetAreCaseInsensitive() {
        Assertions.assertEquals(
                "(EXCLUDED.\"name\" IS NOT NULL AND EXCLUDED.\"version\" >= target.\"version\")",
                render(":name is not null and :version >= TARGET.version"));
    }

    @Test
    void testQuotedColumnNames() {
        Assertions.assertEquals(
                "EXCLUDED.\"Update Time\" > target.\"Update Time\"",
                render(":\"Update Time\" > target.\"Update Time\""));
        // A doubled quote is an escaped quote inside the name.
        Assertions.assertEquals(
                "EXCLUDED.\"say\"hi\" = target.\"name\"", render(":\"say\"\"hi\" = target.name"));
    }

    @Test
    void testColumnsAreQuotedWithTheGivenFunction() {
        Assertions.assertEquals(
                "EXCLUDED.\"UPDATE_TIME\" > target.\"UPDATE_TIME\"",
                UpdateCondition.parse(":update_time > target.update_time", COLUMNS)
                        .toSql(
                                column -> "\"" + column.toUpperCase(Locale.ROOT) + "\"",
                                "EXCLUDED",
                                "target"));
    }

    @Test
    void testBlankConditionIsRejected() {
        Assertions.assertTrue(rejectionMessage("   ").contains("must not be blank"));
        Assertions.assertThrows(
                JdbcConnectorException.class, () -> UpdateCondition.parse(null, COLUMNS));
    }

    @Test
    void testUnknownColumnsAreRejected() {
        Assertions.assertTrue(
                rejectionMessage(":updat_time > target.update_time")
                        .contains("unknown column 'updat_time'"));
        Assertions.assertTrue(
                rejectionMessage(":update_time > target.missing")
                        .contains("unknown column 'missing'"));
        // Column names are matched exactly.
        Assertions.assertTrue(
                rejectionMessage(":UPDATE_TIME > target.update_time")
                        .contains("unknown column 'UPDATE_TIME'"));
    }

    @ParameterizedTest
    @CsvSource(
            delimiter = '|',
            quoteCharacter = '~',
            value = {
                // literals
                ":update_time > 100 | literals are not supported",
                ":name = 'x' | literals are not supported",
                // functions and casts
                "COALESCE(:update_time) > target.update_time | functions are not supported",
                ":update_time::bigint > target.update_time | casts (::) are not supported",
                // other identifiers and qualification forms
                ":update_time > update_time | unsupported identifier 'update_time'",
                ":update_time > EXCLUDED.update_time | unsupported identifier 'EXCLUDED'",
                ":update_time > t.update_time | unsupported identifier 't'",
                ":update_time > target.public.update_time | unsupported qualification",
                ":t.update_time > target.update_time | unsupported qualification",
                ":update_time > target | unsupported identifier 'target'",
                "\"update_time\" > target.update_time | must be written as",
                ": update_time > target.update_time | expected a column name",
                // unsupported operators and characters
                ":update_time != target.update_time | unsupported character '!'",
                ":update_time > target.update_time; DROP TABLE x | unsupported character ';'",
                ":update_time > target.update_time -- x | unsupported character '-'",
                ":update_time > target.update_time + 1 | unsupported character '+'",
                // incomplete or malformed conditions
                ":update_time | expected a comparison",
                ":update_time > | expected :column or target.column",
                "(:update_time > target.update_time | expected ')'",
                ":update_time > target.update_time) | expected end of the condition",
                ":update_time IS target.update_time | expected NULL",
                ":update_time > target.update_time AND | expected :column or target.column",
                ":\"update_time > target.update_time | unterminated quoted identifier",
                ":\"\" > target.update_time | empty quoted identifier"
            })
    void testUnsupportedSyntaxIsRejected(String condition, String expectedReason) {
        String message = rejectionMessage(condition);
        Assertions.assertTrue(
                message.contains(expectedReason),
                () -> "Expected '" + expectedReason + "' in: " + message);
        Assertions.assertTrue(message.contains("update_condition_sql"), message);
    }

    @Test
    void testNestingIsBounded() {
        StringBuilder condition = new StringBuilder();
        for (int i = 0; i < 40; i++) {
            condition.append('(');
        }
        condition.append(":version > target.version");
        for (int i = 0; i < 40; i++) {
            condition.append(')');
        }
        Assertions.assertTrue(rejectionMessage(condition.toString()).contains("nested more than"));
    }
}

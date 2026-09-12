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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DryRunConnectFailureMessageSanitizerTest {

    @Test
    public void testStripJdbcUrlUserInfo() {
        String sanitized =
                sanitize(
                        "Connection failed for jdbc:mysql://alice:secret-password@db.example.com:3306/orders");

        Assertions.assertEquals("Connection failed for the configured JDBC URL", sanitized);
    }

    @Test
    public void testMaskSensitiveQueryParameters() {
        String sanitized =
                sanitize(
                        "Connection failed for jdbc:postgresql://db.example.com:5432/orders?"
                                + "password=secret-password&token=secret-token&ssl=true");

        Assertions.assertTrue(
                sanitized.contains("Connection failed for the configured JDBC URL"),
                "Actual: " + sanitized);
        Assertions.assertFalse(sanitized.contains("secret-password"), "Actual: " + sanitized);
        Assertions.assertFalse(sanitized.contains("secret-token"), "Actual: " + sanitized);
    }

    @Test
    public void testMaskSensitiveFreeTextKeys() {
        String sanitized =
                sanitize(
                        "Unable to connect: password=secret-password, token: secret-token, "
                                + "apiKey='secret api key'");

        Assertions.assertEquals(
                "Unable to connect: password=***, token: ***, apiKey=***", sanitized);
    }

    @Test
    public void testMaskSensitiveQuotedKeys() {
        String sanitized =
                sanitize(
                        "Connection config: {\"username\": \"alice\", "
                                + "\"password\": \"secret password\", "
                                + "'api_key': 'secret key'}");

        Assertions.assertEquals(
                "Connection config: {\"username\": \"alice\", "
                        + "\"password\": ***, 'api_key': ***}",
                sanitized);
    }

    @Test
    public void testReplaceGenericJdbcUrlOutsideKnownFailurePhrases() {
        String sanitized =
                sanitize(
                        "Parser rejected jdbc:duckdb:/tmp/orders.duckdb?api_key=secret-key "
                                + "while opening catalog");

        Assertions.assertEquals(
                "Parser rejected the configured JDBC URL while opening catalog", sanitized);
        Assertions.assertFalse(sanitized.contains("jdbc:"), "Actual: " + sanitized);
        Assertions.assertFalse(sanitized.contains("secret-key"), "Actual: " + sanitized);
    }

    @Test
    public void testReplaceJdbcUrlAfterDriverFailurePhrases() {
        String sanitized =
                sanitize(
                        "No suitable driver found for "
                                + "jdbc:mysql://alice:secret-password@db.example.com:3306/orders?"
                                + "token=secret-token");

        Assertions.assertEquals("No suitable driver found for the configured JDBC URL", sanitized);
    }

    @Test
    public void testReplaceJdbcUrlAfterCatalogFailurePhrases() {
        String sanitized =
                sanitize(
                        "Failed connecting to "
                                + "jdbc:postgresql://db.example.com:5432/orders?password=secret-password "
                                + "via JDBC.");

        Assertions.assertEquals(
                "Failed connecting to the configured JDBC URL via JDBC.", sanitized);
    }

    @Test
    public void testBenignMessageUnchanged() {
        String message = "simulated connection failure: invalid credentials";

        Assertions.assertEquals(message, sanitize(message));
    }

    @Test
    public void testNullAndEmptyMessagesUnchanged() {
        Assertions.assertNull(sanitize(null));
        Assertions.assertEquals("", sanitize(""));
    }

    private static String sanitize(String message) {
        return DryRunConnectFailureMessageSanitizer.sanitize(message);
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.http.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class HttpParameterCredentialSchemeTest {

    private static HttpParameter parameter(String url, String... headerKeys) {
        HttpParameter p = new HttpParameter();
        p.setUrl(url);
        Map<String, String> headers = new HashMap<>();
        for (String key : headerKeys) {
            headers.put(key, "secret-value");
        }
        p.setHeaders(headers);
        return p;
    }

    @Test
    void testAuthorizationHeaderIsDetected() {
        Assertions.assertTrue(
                parameter("http://example.com", "Authorization").hasCredentialHeader());
        Assertions.assertTrue(
                parameter("http://example.com", "authorization").hasCredentialHeader());
    }

    @Test
    void testNonAuthorizationCredentialHeadersAreDetected() {
        // Regression for the substring-only check: these header names never contained
        // "authorization" and were previously silently uncovered.
        Assertions.assertTrue(
                parameter("http://example.com", "PRIVATE-TOKEN").hasCredentialHeader());
        Assertions.assertTrue(parameter("http://example.com", "x-api-key").hasCredentialHeader());
        Assertions.assertTrue(parameter("http://example.com", "api-key").hasCredentialHeader());
    }

    @Test
    void testNonCredentialHeaderIsNotDetected() {
        Assertions.assertFalse(parameter("http://example.com", "Accept").hasCredentialHeader());
        Assertions.assertFalse(
                parameter("http://example.com", "Content-Type").hasCredentialHeader());
        Assertions.assertFalse(parameter("http://example.com").hasCredentialHeader());
    }

    @Test
    void testExplicitCredentialKeyIsDetected() {
        HttpParameter p = parameter("http://example.com", "X-Custom-Token");
        Assertions.assertTrue(
                p.hasCredentialHeader(
                        new java.util.HashSet<>(Collections.singletonList("x-custom-token"))));
    }

    @Test
    void testValidateCredentialSchemeDoesNotThrow() {
        // The validator only logs; it must not throw for any scheme/header combination.
        Assertions.assertDoesNotThrow(
                () -> parameter("http://example.com", "PRIVATE-TOKEN").validateCredentialScheme());
        Assertions.assertDoesNotThrow(
                () -> parameter("https://example.com", "Authorization").validateCredentialScheme());
        Assertions.assertDoesNotThrow(
                () -> parameter("http://example.com").validateCredentialScheme());
        Assertions.assertDoesNotThrow(
                () -> parameter(null, "Authorization").validateCredentialScheme());
    }
}

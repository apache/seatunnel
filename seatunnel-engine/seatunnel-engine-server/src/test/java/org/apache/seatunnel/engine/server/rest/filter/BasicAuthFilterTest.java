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

package org.apache.seatunnel.engine.server.rest.filter;

import org.apache.seatunnel.engine.common.config.server.HttpConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Covers which credentials the basic auth filter accepts, and that a credential it cannot compare
 * is refused rather than throwing.
 */
public class BasicAuthFilterTest {

    private static final String USER = "admin";
    private static final String PASSWORD = "s3cret";

    @Test
    void testCorrectCredentialsPassThroughTheChain() throws Exception {
        AtomicInteger chainCalls = new AtomicInteger();
        HttpServletResponse response = mock(HttpServletResponse.class);

        doFilterWith(config(USER, PASSWORD), USER, PASSWORD, response, chainCalls);

        Assertions.assertEquals(1, chainCalls.get());
        verify(response, never()).sendError(HttpServletResponse.SC_UNAUTHORIZED, "Unauthorized");
    }

    @Test
    void testWrongPasswordIsRejected() throws Exception {
        AtomicInteger chainCalls = new AtomicInteger();
        HttpServletResponse response = mock(HttpServletResponse.class);

        doFilterWith(config(USER, PASSWORD), USER, "wrong", response, chainCalls);

        Assertions.assertEquals(0, chainCalls.get());
        verify(response).sendError(HttpServletResponse.SC_UNAUTHORIZED, "Unauthorized");
    }

    @Test
    void testWrongUsernameIsRejected() throws Exception {
        AtomicInteger chainCalls = new AtomicInteger();
        HttpServletResponse response = mock(HttpServletResponse.class);

        doFilterWith(config(USER, PASSWORD), "root", PASSWORD, response, chainCalls);

        Assertions.assertEquals(0, chainCalls.get());
        verify(response).sendError(HttpServletResponse.SC_UNAUTHORIZED, "Unauthorized");
    }

    @Test
    void testCredentialThatIsAPrefixOfTheConfiguredOneIsRejected() throws Exception {
        // A prefix is the shape a timing probe walks through one character at a time, so pin
        // that it is refused rather than accepted on the characters that do match.
        AtomicInteger chainCalls = new AtomicInteger();
        HttpServletResponse response = mock(HttpServletResponse.class);

        doFilterWith(config(USER, PASSWORD), USER, "s3cre", response, chainCalls);

        Assertions.assertEquals(0, chainCalls.get());
        verify(response).sendError(HttpServletResponse.SC_UNAUTHORIZED, "Unauthorized");
    }

    @Test
    void testUnsetCredentialIsRejectedAndDoesNotThrow() throws Exception {
        // The configured values are compared as bytes now, so an unset credential must be
        // refused explicitly. Reading bytes off a null would turn a 401 into a 500.
        for (HttpConfig config :
                new HttpConfig[] {config(null, PASSWORD), config(USER, null), config(null, null)}) {
            AtomicInteger chainCalls = new AtomicInteger();
            HttpServletResponse response = mock(HttpServletResponse.class);

            Assertions.assertDoesNotThrow(
                    () -> doFilterWith(config, USER, PASSWORD, response, chainCalls));

            Assertions.assertEquals(0, chainCalls.get());
            verify(response).sendError(HttpServletResponse.SC_UNAUTHORIZED, "Unauthorized");
        }
    }

    @Test
    void testAuthenticationDisabledSkipsTheCredentialCheckEntirely() throws Exception {
        HttpConfig config = new HttpConfig();
        config.setEnableBasicAuth(false);
        AtomicInteger chainCalls = new AtomicInteger();
        HttpServletResponse response = mock(HttpServletResponse.class);

        doFilterWith(config, "anyone", "anything", response, chainCalls);

        Assertions.assertEquals(1, chainCalls.get());
        verify(response, never()).sendError(HttpServletResponse.SC_UNAUTHORIZED, "Unauthorized");
    }

    private static HttpConfig config(String username, String password) {
        HttpConfig config = new HttpConfig();
        config.setEnableBasicAuth(true);
        config.setBasicAuthUsername(username);
        config.setBasicAuthPassword(password);
        return config;
    }

    private static void doFilterWith(
            HttpConfig config,
            String username,
            String password,
            HttpServletResponse response,
            AtomicInteger chainCalls)
            throws Exception {
        String header =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        (username + ":" + password)
                                                .getBytes(StandardCharsets.UTF_8));
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getHeader("Authorization")).thenReturn(header);

        FilterChain chain = (req, resp) -> chainCalls.incrementAndGet();
        new BasicAuthFilter(config).doFilter(request, response, chain);
    }
}

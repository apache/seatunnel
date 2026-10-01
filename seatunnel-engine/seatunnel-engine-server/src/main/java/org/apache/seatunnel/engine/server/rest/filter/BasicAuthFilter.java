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

import org.apache.commons.codec.binary.Base64;

import lombok.extern.slf4j.Slf4j;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** Basic authentication filter for the web UI. */
@Slf4j
public class BasicAuthFilter implements Filter {

    private final HttpConfig httpConfig;
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BASIC_PREFIX = "Basic ";
    private static final String WWW_AUTHENTICATE_HEADER = "WWW-Authenticate";
    private static final String BASIC_REALM = "Basic realm=\"SeaTunnel Web UI\"";

    public BasicAuthFilter(HttpConfig httpConfig) {
        this.httpConfig = httpConfig;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // No initialization needed
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        // Skip authentication if not enabled
        if (!httpConfig.isEnableBasicAuth()) {
            chain.doFilter(request, response);
            return;
        }

        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        // Get the Authorization header from the request
        String authHeader = httpRequest.getHeader(AUTHORIZATION_HEADER);

        // Check if the Authorization header exists and starts with "Basic "
        if (authHeader != null && authHeader.startsWith(BASIC_PREFIX)) {
            // Extract the Base64 encoded username:password
            String base64Credentials = authHeader.substring(BASIC_PREFIX.length());
            String credentials =
                    new String(Base64.decodeBase64(base64Credentials), StandardCharsets.UTF_8);

            // Split the username and password
            final String[] values = credentials.split(":", 2);
            if (values.length == 2) {
                String username = values[0];
                String password = values[1];

                // Both comparisons are evaluated before the branch, on purpose. String.equals
                // returns at the first differing character, and && skips the password
                // comparison entirely when the username is wrong, so response time separates a
                // wrong username from a correct username with a wrong password, and grows with
                // the number of leading characters guessed correctly (CWE-208).
                boolean usernameMatches =
                        credentialMatches(username, httpConfig.getBasicAuthUsername());
                boolean passwordMatches =
                        credentialMatches(password, httpConfig.getBasicAuthPassword());
                if (usernameMatches && passwordMatches) {
                    // Authentication successful, proceed with the request
                    chain.doFilter(request, response);
                    return;
                }
            }
        }

        // Authentication failed, send 401 Unauthorized response
        httpResponse.setHeader(WWW_AUTHENTICATE_HEADER, BASIC_REALM);
        httpResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Unauthorized");
    }

    /**
     * Compares a credential taken from the request against the configured one without leaking,
     * through timing, how many leading bytes matched.
     *
     * <p>MessageDigest.isEqual accumulates the difference over every byte instead of returning at
     * the first mismatch. It reads exactly as many bytes as its first argument holds, so the
     * request's own value is passed first: the loop length is then something the caller already
     * knows, and the length of the configured credential is not revealed.
     *
     * @param provided the value supplied by the request, never null at the call sites below
     * @param expected the configured value, which is null when the credential is not set
     * @return true only when the credential is configured and the two values are equal
     */
    private static boolean credentialMatches(String provided, String expected) {
        if (expected == null) {
            return false;
        }
        return MessageDigest.isEqual(
                provided.getBytes(StandardCharsets.UTF_8),
                expected.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void destroy() {
        // No resources to release
    }
}

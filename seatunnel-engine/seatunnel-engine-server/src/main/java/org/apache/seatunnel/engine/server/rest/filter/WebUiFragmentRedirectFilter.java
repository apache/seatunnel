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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

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

/**
 * Restores the URL fragment of Web UI routes that a client percent-encoded into the request path.
 *
 * <p>The Web UI is a single page application that keeps its route in the URL fragment, so a deep
 * link looks like {@code http://host:8080/#/overview}. A client that escapes the whole URL sends
 * the fragment as part of the path instead ({@code /%23/overview}): Safari does that when it
 * re-requests a hash route, and so do tools that escape a whole URL, such as a chat client, a wiki
 * page or a markdown preview. No static resource matches that path, so the Web UI answered with the
 * bare Jetty "HTTP ERROR 404" page while the address bar still showed {@code #/overview}. This
 * filter recognizes that shape and redirects the client to the URL it meant to open, which the Web
 * UI router resolves as usual.
 *
 * <p>A fragment sent unencoded never reaches the server (the client keeps it), so only the
 * percent-encoded form is handled here. The route and the query are only re-emitted when they are
 * well-formed URI text; a request that carries anything else is left to the normal handling, so the
 * {@code Location} value never holds bytes the client did not send as a URI. Every other request
 * passes through untouched.
 */
@Slf4j
public class WebUiFragmentRedirectFilter implements Filter {

    /** A percent-encoded {@code #}, as produced by clients that escape a whole URL. */
    private static final String ENCODED_FRAGMENT_PATH_PREFIX = "/%23";

    /** URI characters a hash route or its query may carry without escaping. */
    private static final String ROUTE_CHARACTERS = "-._~!$&'()*+,;=:@/?";

    private static final String GET = "GET";

    private static final String HEAD = "HEAD";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // No initialization needed
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (!(request instanceof HttpServletRequest)
                || !(response instanceof HttpServletResponse)) {
            chain.doFilter(request, response);
            return;
        }

        HttpServletRequest httpRequest = (HttpServletRequest) request;
        String location =
                redirectLocationFor(
                        httpRequest.getMethod(),
                        httpRequest.getRequestURI(),
                        httpRequest.getContextPath(),
                        httpRequest.getQueryString());
        if (location == null) {
            chain.doFilter(request, response);
            return;
        }

        log.debug(
                "Redirecting {} to {} to restore the Web UI route the client encoded",
                httpRequest.getRequestURI(),
                location);
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        httpResponse.setStatus(HttpServletResponse.SC_FOUND);
        httpResponse.setHeader("Location", location);
    }

    @Override
    public void destroy() {
        // No resources to release
    }

    /**
     * Builds the Web UI URL a request meant to open when its fragment arrived percent-encoded.
     *
     * @param method the HTTP method of the request
     * @param requestUri the raw, still percent-encoded request URI
     * @param contextPath the context path the request was served under, empty when there is none
     * @param queryString the query string of the request, {@code null} when there is none
     * @return the location to redirect to, or {@code null} when the request carries no encoded
     *     fragment and must be handled normally
     */
    static String redirectLocationFor(
            String method, String requestUri, String contextPath, String queryString) {
        if (!GET.equals(method) && !HEAD.equals(method)) {
            return null;
        }
        if (requestUri == null || contextPath == null || !requestUri.startsWith(contextPath)) {
            return null;
        }
        String pathInContext = requestUri.substring(contextPath.length());
        if (!pathInContext.startsWith(ENCODED_FRAGMENT_PATH_PREFIX)) {
            return null;
        }

        String fragment = pathInContext.substring(ENCODED_FRAGMENT_PATH_PREFIX.length());
        if (!isUriText(fragment) || !isUriText(queryString)) {
            log.debug(
                    "Not redirecting {}: the encoded fragment is not a URI reference", requestUri);
            return null;
        }

        StringBuilder location = new StringBuilder(contextPath).append('/');
        if (!fragment.isEmpty()) {
            // The client kept the route after the encoded separator, so it is the fragment again.
            location.append('#').append(fragment);
        }
        if (StringUtils.isNotBlank(queryString)) {
            location.append('?').append(queryString);
        }
        return location.toString();
    }

    /**
     * Checks that a route fragment or a query string is well-formed URI text: URI characters, with
     * {@code %} only as part of a {@code %HH} escape that is neither a control character nor {@code
     * DEL}.
     *
     * <p>A hash route never needs anything else, so a raw {@code <}, an encoded line break or a
     * truncated escape means the caller did not send a Web UI URL and the request is answered
     * normally. Keeping this check here means the header value is well formed regardless of what
     * the HTTP layer accepted on the request line.
     *
     * @param text the raw fragment or query of the request, {@code null} when there is none
     * @return {@code true} when the text may be re-emitted in a {@code Location} header
     */
    private static boolean isUriText(String text) {
        if (text == null) {
            return true;
        }
        for (int i = 0; i < text.length(); i++) {
            char character = text.charAt(i);
            if (character == '%') {
                // A truncated or non-hex escape cannot be part of a URI reference.
                if (i + 2 >= text.length()) {
                    return false;
                }
                int high = hexValue(text.charAt(i + 1));
                int low = hexValue(text.charAt(i + 2));
                if (high < 0 || low < 0) {
                    return false;
                }
                int escaped = (high << 4) + low;
                if (escaped < 0x20 || escaped == 0x7F) {
                    return false;
                }
                i += 2;
            } else if (!isRouteCharacter(character)) {
                return false;
            }
        }
        return true;
    }

    /** @return the value of an ASCII hex digit, or {@code -1} for anything else */
    private static int hexValue(char character) {
        if (character >= '0' && character <= '9') {
            return character - '0';
        }
        if (character >= 'a' && character <= 'f') {
            return character - 'a' + 10;
        }
        if (character >= 'A' && character <= 'F') {
            return character - 'A' + 10;
        }
        return -1;
    }

    private static boolean isRouteCharacter(char character) {
        return (character >= 'a' && character <= 'z')
                || (character >= 'A' && character <= 'Z')
                || (character >= '0' && character <= '9')
                || ROUTE_CHARACTERS.indexOf(character) >= 0;
    }
}

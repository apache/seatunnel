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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.singlestore;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectFactory;

import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Factory for {@link SingleStoreDialect}. */
@AutoService(JdbcDialectFactory.class)
public class SingleStoreDialectFactory implements JdbcDialectFactory {

    private static final String URL_PREFIX = "jdbc:singlestore:";
    /** Expect: jdbc:singlestore:[loadbalance:|sequential:]//host[:port][/database][?params]. */
    private static final Pattern URL_PATTERN =
            Pattern.compile(
                    "^jdbc:singlestore:(?:loadbalance:|sequential:)?//([^/?#]+)(/[^?#]*)?.*");

    @Override
    public String dialectFactoryName() {
        return DatabaseIdentifier.SINGLESTORE;
    }

    /**
     * Accepts URLs with prefix {@code jdbc:singlestore:} and valid format: at least host present
     * after {@code //} (e.g. {@code jdbc:singlestore://host:3306/db} or {@code
     * jdbc:singlestore:loadbalance://h1,h2/db}).
     */
    @Override
    public boolean acceptsURL(String url) {
        if (url == null || !url.startsWith(URL_PREFIX)) {
            return false;
        }
        Matcher m = URL_PATTERN.matcher(url);
        if (!m.matches()) {
            return false;
        }
        String hostPart = m.group(1);
        if (hostPart == null || hostPart.trim().isEmpty()) {
            return false;
        }

        String[] hosts = hostPart.split(",");
        for (String host : hosts) {
            String trimmedHost = host.trim();
            if (trimmedHost.isEmpty()) {
                return false;
            }
            if (!isValidHostSegment(trimmedHost)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public JdbcDialect create() {
        return new SingleStoreDialect();
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        return new SingleStoreDialect(fieldIde);
    }

    /**
     * Validates an individual host segment without rejecting bracketed IPv6 literals that omit a
     * port.
     */
    private boolean isValidHostSegment(String hostSegment) {
        if (hostSegment.startsWith("[")) {
            return isValidBracketedIpv6Host(hostSegment);
        }
        int firstColonIndex = hostSegment.indexOf(':');
        int lastColonIndex = hostSegment.lastIndexOf(':');
        if (firstColonIndex < 0) {
            return true;
        }
        if (firstColonIndex != lastColonIndex) {
            // Leave ambiguous unbracketed IPv6-like segments to the JDBC driver.
            return true;
        }
        return isValidPort(hostSegment.substring(lastColonIndex + 1));
    }

    /** Validates bracketed IPv6 hosts with an optional numeric port suffix. */
    private boolean isValidBracketedIpv6Host(String hostSegment) {
        int closingBracketIndex = hostSegment.indexOf(']');
        if (closingBracketIndex <= 1) {
            return false;
        }
        if (closingBracketIndex == hostSegment.length() - 1) {
            return true;
        }
        if (hostSegment.charAt(closingBracketIndex + 1) != ':') {
            return false;
        }
        return isValidPort(hostSegment.substring(closingBracketIndex + 2));
    }

    /** Validates a JDBC port number when the URL makes the port position unambiguous. */
    private boolean isValidPort(String portPart) {
        if (portPart.isEmpty()) {
            return false;
        }
        try {
            int port = Integer.parseInt(portPart);
            return port >= 1 && port <= 65535;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}

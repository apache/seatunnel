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

package org.apache.seatunnel.engine.server.rest.servlet;

import org.apache.seatunnel.shade.org.eclipse.jetty.server.Connector;
import org.apache.seatunnel.shade.org.eclipse.jetty.server.Server;
import org.apache.seatunnel.shade.org.eclipse.jetty.server.ServerConnector;
import org.apache.seatunnel.shade.org.eclipse.jetty.server.SslConnectionFactory;

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

/**
 * Returns safe, read-only HTTP service status without exposing credentials or certificate paths.
 */
public class HttpServiceStatusServlet extends BaseServlet {

    private final SeaTunnelConfig seaTunnelConfig;
    private final Server server;

    /**
     * Creates a status servlet bound to the current Jetty server instance.
     *
     * @param nodeEngine Hazelcast node engine used by REST servlet base class.
     * @param seaTunnelConfig runtime configuration containing HTTP service options.
     * @param server Jetty server whose connectors expose effective listener ports.
     */
    public HttpServiceStatusServlet(
            NodeEngineImpl nodeEngine, SeaTunnelConfig seaTunnelConfig, Server server) {
        super(nodeEngine);
        this.seaTunnelConfig = seaTunnelConfig;
        this.server = server;
    }

    /**
     * Handles safe HTTP service status inspection for the current node.
     *
     * <p>The response intentionally reports only switches, ports, context path, and port range. It
     * must not include passwords, usernames, keystore paths, or truststore paths.
     */
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        HttpConfig httpConfig = seaTunnelConfig.getEngineConfig().getHttpConfig();
        JsonObject status =
                new JsonObject()
                        .add("httpEnabled", httpConfig.isEnabled())
                        .add("httpsEnabled", httpConfig.isEnableHttps())
                        .add("configuredHttpPort", httpConfig.getPort())
                        .add("configuredHttpsPort", httpConfig.getHttpsPort())
                        .add("httpPort", findConnectorPort(false, httpConfig.getPort()))
                        .add("httpsPort", findConnectorPort(true, httpConfig.getHttpsPort()))
                        .add("contextPath", defaultContextPath(httpConfig.getContextPath()))
                        .add("dynamicPortEnabled", httpConfig.isEnableDynamicPort())
                        .add("portRange", httpConfig.getPortRange())
                        .add("basicAuthEnabled", httpConfig.isEnableBasicAuth())
                        .add(
                                "mutualTlsEnabled",
                                hasText(httpConfig.getTrustStorePath())
                                        && hasText(httpConfig.getTrustStorePassword()));
        writeJson(resp, status);
    }

    /**
     * Returns the effective listener port for an HTTP or HTTPS connector.
     *
     * @param ssl whether to find an SSL connector.
     * @param configuredPort fallback port from configuration.
     * @return effective local port when available, otherwise the configured port.
     */
    private int findConnectorPort(boolean ssl, int configuredPort) {
        for (Connector connector : server.getConnectors()) {
            if (!(connector instanceof ServerConnector)) {
                continue;
            }
            ServerConnector serverConnector = (ServerConnector) connector;
            if (hasSslConnectionFactory(serverConnector) == ssl) {
                return serverConnector.getLocalPort() > 0
                        ? serverConnector.getLocalPort()
                        : serverConnector.getPort();
            }
        }
        return configuredPort;
    }

    /**
     * Normalizes blank context paths for UI display.
     *
     * @param contextPath configured context path.
     * @return configured path, or "/" when it is blank.
     */
    private String defaultContextPath(String contextPath) {
        return hasText(contextPath) ? contextPath : "/";
    }

    /**
     * Checks whether a connector is backed by SSL.
     *
     * @param connector Jetty server connector to inspect.
     * @return true when the connector has an SSL connection factory.
     */
    private boolean hasSslConnectionFactory(ServerConnector connector) {
        return connector.getConnectionFactories().stream()
                .anyMatch(factory -> factory instanceof SslConnectionFactory);
    }

    /**
     * Returns whether a string contains non-whitespace text.
     *
     * @param value value to inspect.
     * @return true if the value is non-null and non-blank.
     */
    private boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}

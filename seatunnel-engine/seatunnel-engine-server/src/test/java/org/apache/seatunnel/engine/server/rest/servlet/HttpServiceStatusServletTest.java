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

import org.apache.seatunnel.shade.org.eclipse.jetty.server.Server;

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;

import org.junit.jupiter.api.Test;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that HTTP status responses expose only derived operational values and never include
 * sensitive configuration values, even when such values are configured.
 */
class HttpServiceStatusServletTest {

    /**
     * Verifies that derived status can be returned without serializing secret configuration, which
     * protects credentials and certificate paths from the operations UI.
     */
    @Test
    void shouldNotExposeSensitiveHttpConfiguration() throws IOException, ServletException {
        SeaTunnelConfig seaTunnelConfig = new SeaTunnelConfig();
        HttpConfig httpConfig = seaTunnelConfig.getEngineConfig().getHttpConfig();
        httpConfig.setKeyStorePath("/secrets/key-store.p12");
        httpConfig.setKeyStorePassword("key-store-password");
        httpConfig.setKeyManagerPassword("key-manager-password");
        httpConfig.setTrustStorePath("/secrets/trust-store.p12");
        httpConfig.setTrustStorePassword("trust-store-password");
        httpConfig.setBasicAuthUsername("operator");
        httpConfig.setBasicAuthPassword("basic-auth-password");

        HttpServletResponse response = mock(HttpServletResponse.class);
        StringWriter body = new StringWriter();
        when(response.getWriter()).thenReturn(new PrintWriter(body));

        new HttpServiceStatusServlet(null, seaTunnelConfig, new Server()).doGet(null, response);

        String status = body.toString();
        assertTrue(status.contains("\"mutualTlsEnabled\":true"));
        assertFalse(status.contains("/secrets/key-store.p12"));
        assertFalse(status.contains("key-store-password"));
        assertFalse(status.contains("key-manager-password"));
        assertFalse(status.contains("/secrets/trust-store.p12"));
        assertFalse(status.contains("trust-store-password"));
        assertFalse(status.contains("operator"));
        assertFalse(status.contains("basic-auth-password"));
    }
}

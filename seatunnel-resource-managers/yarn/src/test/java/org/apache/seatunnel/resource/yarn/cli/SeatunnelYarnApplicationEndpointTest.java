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

package org.apache.seatunnel.resource.yarn.cli;

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;

import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SeatunnelYarnApplicationEndpointTest {
    private final SeaTunnelConfig engine = new SeaTunnelConfig();
    private final ApplicationSpecification spec =
            SeatunnelApplicationConfig.parse("test-job", Collections.emptyMap());

    @Test
    void usesOnlyHttpsOnTheFixedPortWhenTlsIsConfigured() throws URISyntaxException {
        HttpConfig http = new HttpConfig();
        http.setEnabled(true);
        http.setEnableHttps(true);
        http.setEnableBasicAuth(true);
        http.setKeyStorePath("am.p12");
        http.setContextPath("/seatunnel");
        engine.getEngineConfig().setHttpConfig(http);

        SeatunnelYarnApplicationCli.configureEngine(engine, "app", spec, 8095);

        HttpConfig endpoint = engine.getEngineConfig().getHttpConfig();
        assertFalse(endpoint.isEnabled());
        assertTrue(endpoint.isEnableHttps());
        assertEquals(8095, endpoint.getHttpsPort());
        assertEquals("am.p12", endpoint.getKeyStorePath());
        assertTrue(endpoint.isEnableBasicAuth());
        assertEquals(
                "https://am.example:8095/seatunnel/",
                SeatunnelYarnApplicationCli.trackingUrl(endpoint, "am.example"));
    }

    @Test
    void usesFixedHttpPortByDefault() throws URISyntaxException {
        SeatunnelYarnApplicationCli.configureEngine(engine, "app", spec, 8095);

        HttpConfig endpoint = engine.getEngineConfig().getHttpConfig();
        assertTrue(endpoint.isEnabled());
        assertFalse(endpoint.isEnableHttps());
        assertFalse(endpoint.isEnableDynamicPort());
        assertEquals(8095, endpoint.getPort());
        assertEquals(
                "http://am.example:8095/",
                SeatunnelYarnApplicationCli.trackingUrl(endpoint, "am.example"));
    }

    @Test
    void disablesBothListenersWhenNoEndpointWasRequested() throws URISyntaxException {
        HttpConfig http = new HttpConfig();
        http.setEnabled(true);
        http.setEnableHttps(true);
        engine.getEngineConfig().setHttpConfig(http);

        SeatunnelYarnApplicationCli.configureEngine(engine, "app", spec, null);

        HttpConfig endpoint = engine.getEngineConfig().getHttpConfig();
        assertFalse(endpoint.isEnabled());
        assertFalse(endpoint.isEnableHttps());
        assertEquals("", SeatunnelYarnApplicationCli.trackingUrl(endpoint, "am.example"));
    }
}

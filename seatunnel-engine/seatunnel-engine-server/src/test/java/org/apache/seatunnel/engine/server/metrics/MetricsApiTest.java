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

package org.apache.seatunnel.engine.server.metrics;

import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;

@DisabledOnOs(OS.WINDOWS)
public class MetricsApiTest {

    private static HazelcastInstanceImpl instance;

    @BeforeAll
    public static void before() {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getEngineConfig().getTelemetryConfig().getMetric().setEnabled(true);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(true);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setPort(8080);
        seaTunnelConfig.getEngineConfig().setMode(ExecutionMode.LOCAL);
        instance = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
    }

    @Test
    public void metricsApiTest() {
        given().get("http://localhost:8080" + RestConstant.REST_URL_METRICS)
                .then()
                .statusCode(200)
                .body(containsString("process_start_time_seconds"));
    }

    @AfterAll
    public static void after() {
        if (instance != null) {
            instance.shutdown();
        }
    }
}

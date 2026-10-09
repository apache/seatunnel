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

package org.apache.seatunnel.lineage;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LineageConfigTest {

    @Test
    void resolvesEachOptionByPriority() {
        Map<String, Object> job = new HashMap<>();
        job.put(LineageConfig.ENABLED, true);
        job.put(LineageConfig.TIMEOUT_MS, 2000);
        Map<String, Object> cluster = new HashMap<>();
        cluster.put("openlineage.timeout_ms", 3000);
        cluster.put("openlineage.namespace", "cluster");
        Map<String, Object> environment = new HashMap<>();
        environment.put("OPENLINEAGE_NAMESPACE", "environment");

        LineageConfig config = LineageConfig.resolve(job, cluster, environment);

        assertEquals(true, config.enabled());
        assertEquals(2000, config.timeoutMs());
        assertEquals("environment", config.namespace());
        assertFalse(config.toString().contains("secret"));
    }

    /**
     * The producer default must be derived from the running version rather than from a build-time
     * constant, which would silently keep reporting a stale version after a release. Setting the
     * version property must therefore change the resolved producer.
     */
    @Test
    void producerDefaultFollowsTheRunningVersion() {
        String originalVersion = System.getProperty("seatunnel.version");
        try {
            System.setProperty("seatunnel.version", "9.9.9");
            assertEquals("https://seatunnel.apache.org/9.9.9", LineageConfig.defaults().producer());

            System.setProperty("seatunnel.version", "");
            String withoutVersion = LineageConfig.defaults().producer();
            assertEquals("https://seatunnel.apache.org/", withoutVersion);
        } finally {
            if (originalVersion == null) {
                System.clearProperty("seatunnel.version");
            } else {
                System.setProperty("seatunnel.version", originalVersion);
            }
        }
    }

    @Test
    void tokenUsesOnlyEnvironmentThenCluster() {
        Map<String, Object> cluster =
                Collections.singletonMap("openlineage.auth_token", "cluster-token");
        Map<String, Object> environment =
                Collections.singletonMap("OPENLINEAGE_AUTH_TOKEN", "env-token");

        assertEquals("env-token", LineageConfig.resolveToken(cluster, environment));
        assertEquals("cluster-token", LineageConfig.resolveToken(cluster, Collections.emptyMap()));

        LineageConfig config = LineageConfig.resolve(Collections.emptyMap(), cluster, environment);
        assertFalse(config.toString().contains("env-token"));
        assertFalse(config.withAuthToken(null).toMap().containsKey(LineageConfig.AUTH_TOKEN));
    }

    @Test
    void rejectsTokenInJobOptions() {
        Map<String, Object> job =
                Collections.singletonMap(LineageConfig.AUTH_TOKEN, "not-a-real-token");

        assertThrows(
                IllegalArgumentException.class,
                () -> LineageConfig.resolve(job, Collections.emptyMap(), Collections.emptyMap()));
    }

    @Test
    void parsesStringRunPropertiesWithSourcePriority() {
        Map<String, Object> cluster = new HashMap<>();
        cluster.put("openlineage.run_properties", "cluster: value, shared: cluster");
        Map<String, Object> environment = new HashMap<>();
        environment.put("OPENLINEAGE_RUN_PROPERTIES", "{environment: value, shared: environment}");

        LineageConfig environmentConfig =
                LineageConfig.resolve(Collections.emptyMap(), cluster, environment);

        assertEquals("value", environmentConfig.runProperties().get("environment"));
        assertEquals("environment", environmentConfig.runProperties().get("shared"));
        assertFalse(environmentConfig.runProperties().containsKey("cluster"));

        LineageConfig clusterConfig =
                LineageConfig.resolve(Collections.emptyMap(), cluster, Collections.emptyMap());
        assertEquals("value", clusterConfig.runProperties().get("cluster"));
        assertEquals("cluster", clusterConfig.runProperties().get("shared"));
    }

    @Test
    void parsesPrefixedClusterRunProperties() {
        Map<String, Object> cluster = new HashMap<>();
        cluster.put("openlineage.run_properties.owner", "platform");
        cluster.put("openlineage.run_properties.region", "cn");

        LineageConfig config =
                LineageConfig.resolve(Collections.emptyMap(), cluster, Collections.emptyMap());

        assertEquals("platform", config.runProperties().get("owner"));
        assertEquals("cn", config.runProperties().get("region"));
    }

    /**
     * The five string options share one validation path, so an unnamed error leaves the operator
     * guessing which of them was blanked out.
     */
    @Test
    void namesTheBlankOptionItRejects() {
        Map<String, Object> cluster = Collections.singletonMap(LineageConfig.NAMESPACE, "  ");

        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                LineageConfig.resolve(
                                        Collections.emptyMap(), cluster, Collections.emptyMap()));

        assertTrue(
                failure.getMessage().contains(LineageConfig.NAMESPACE),
                "the error must name the option at fault, was: " + failure.getMessage());
    }

    @Test
    void rejectsMalformedStringRunProperties() {
        Map<String, Object> environment =
                Collections.singletonMap("OPENLINEAGE_RUN_PROPERTIES", "not-a-map");

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        LineageConfig.resolve(
                                Collections.emptyMap(), Collections.emptyMap(), environment));
    }

    /**
     * The separators of the string map form are also legal inside a value: a team name carries a
     * comma and a URL carries a colon. Quoting is the only way to write either, so the parser must
     * keep the quoted section intact instead of splitting inside it.
     */
    @Test
    void keepsSeparatorsThatAppearInsideQuotedRunPropertyValues() {
        Map<String, Object> cluster =
                Collections.singletonMap(
                        "openlineage.run_properties",
                        "owner: \"data, platform\", docs: 'http://wiki/x', plain: value");

        LineageConfig config =
                LineageConfig.resolve(Collections.emptyMap(), cluster, Collections.emptyMap());

        assertEquals("data, platform", config.runProperties().get("owner"));
        assertEquals("http://wiki/x", config.runProperties().get("docs"));
        assertEquals("value", config.runProperties().get("plain"));
    }

    /**
     * A duration written the way the rest of SeaTunnel accepts one is the likely mistake here. The
     * configuration is resolved again on every reported event, so a failure that names only the
     * text repeats forever in a log that never says which of the numeric options to correct.
     */
    @Test
    void namesTheNumericOptionThatCouldNotBeParsed() {
        Map<String, Object> job = Collections.singletonMap(LineageConfig.TIMEOUT_MS, "10s");

        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                LineageConfig.resolve(
                                        job, Collections.emptyMap(), Collections.emptyMap()));

        assertTrue(failure.getMessage().contains(LineageConfig.TIMEOUT_MS), failure.getMessage());
        assertTrue(failure.getMessage().contains("10s"), failure.getMessage());
    }

    @Test
    void namesTheHeartbeatOptionThatCouldNotBeParsed() {
        Map<String, Object> cluster =
                Collections.singletonMap("openlineage.heartbeat_min_interval_ms", "1h");

        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                LineageConfig.resolve(
                                        Collections.emptyMap(), cluster, Collections.emptyMap()));

        assertTrue(
                failure.getMessage().contains(LineageConfig.HEARTBEAT_MIN_INTERVAL_MS),
                failure.getMessage());
    }

    @Test
    void rejectsRunPropertiesThatEndInsideAQuote() {
        Map<String, Object> cluster =
                Collections.singletonMap("openlineage.run_properties", "owner: \"unterminated");

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        LineageConfig.resolve(
                                Collections.emptyMap(), cluster, Collections.emptyMap()));
    }
}

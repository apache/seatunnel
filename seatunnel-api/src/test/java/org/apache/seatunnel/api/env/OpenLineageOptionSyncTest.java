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

package org.apache.seatunnel.api.env;

import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.lineage.LineageConfig;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Guards against drift between the env option declarations and {@link LineageConfig} constants.
 *
 * <p>{@link EnvCommonOptions} and {@link LineageConfig} intentionally keep separate copies of the
 * OpenLineage option keys and defaults so that {@code seatunnel-api} does not take a compile-time
 * dependency on {@code seatunnel-lineage}. This test is the low-risk sync check that keeps the two
 * surfaces aligned.
 */
public class OpenLineageOptionSyncTest {

    @Test
    public void openLineageOptionKeysMatchLineageConfig() {
        assertEquals(LineageConfig.ENABLED, EnvCommonOptions.OPENLINEAGE_ENABLED.key());
        assertEquals(LineageConfig.TRANSPORT, EnvCommonOptions.OPENLINEAGE_TRANSPORT.key());
        assertEquals(LineageConfig.URL, EnvCommonOptions.OPENLINEAGE_URL.key());
        assertEquals(LineageConfig.NAMESPACE, EnvCommonOptions.OPENLINEAGE_NAMESPACE.key());
        assertEquals(LineageConfig.AUTH_TOKEN, EnvCommonOptions.OPENLINEAGE_AUTH_TOKEN.key());
        assertEquals(LineageConfig.TIMEOUT_MS, EnvCommonOptions.OPENLINEAGE_TIMEOUT_MS.key());
        assertEquals(LineageConfig.RETRY_TIMES, EnvCommonOptions.OPENLINEAGE_RETRY_TIMES.key());
        assertEquals(LineageConfig.RUN_FACET, EnvCommonOptions.OPENLINEAGE_RUN_FACET.key());
        assertEquals(
                LineageConfig.RUN_PROPERTIES, EnvCommonOptions.OPENLINEAGE_RUN_PROPERTIES.key());
        assertEquals(
                LineageConfig.HEARTBEAT_MIN_INTERVAL_MS,
                EnvCommonOptions.OPENLINEAGE_HEARTBEAT_MIN_INTERVAL_MS.key());
        assertEquals(LineageConfig.PRODUCER, EnvCommonOptions.OPENLINEAGE_PRODUCER.key());
    }

    @Test
    public void openLineageOptionDefaultsMatchLineageConfig() {
        assertEquals(Boolean.FALSE, EnvCommonOptions.OPENLINEAGE_ENABLED.defaultValue());
        assertEquals(
                LineageConfig.DEFAULT_TRANSPORT,
                EnvCommonOptions.OPENLINEAGE_TRANSPORT.defaultValue());
        assertNull(EnvCommonOptions.OPENLINEAGE_URL.defaultValue());
        assertEquals(
                LineageConfig.DEFAULT_NAMESPACE,
                EnvCommonOptions.OPENLINEAGE_NAMESPACE.defaultValue());
        assertNull(EnvCommonOptions.OPENLINEAGE_AUTH_TOKEN.defaultValue());
        assertEquals(
                LineageConfig.DEFAULT_TIMEOUT_MS,
                EnvCommonOptions.OPENLINEAGE_TIMEOUT_MS.defaultValue());
        assertEquals(
                LineageConfig.DEFAULT_RETRY_TIMES,
                EnvCommonOptions.OPENLINEAGE_RETRY_TIMES.defaultValue());
        assertEquals(
                LineageConfig.DEFAULT_RUN_FACET,
                EnvCommonOptions.OPENLINEAGE_RUN_FACET.defaultValue());
        assertNull(EnvCommonOptions.OPENLINEAGE_RUN_PROPERTIES.defaultValue());
        assertEquals(
                LineageConfig.DEFAULT_HEARTBEAT_MIN_INTERVAL_MS,
                EnvCommonOptions.OPENLINEAGE_HEARTBEAT_MIN_INTERVAL_MS.defaultValue());
        // PRODUCER has no baked-in default: LineageConfig resolves it at runtime from the package
        // version so a release does not ship a stale producer URI in the option declaration.
        assertNull(EnvCommonOptions.OPENLINEAGE_PRODUCER.defaultValue());
    }
}

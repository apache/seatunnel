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

package org.apache.seatunnel.engine.server.observability;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ObservabilityConfigTest {

    @Test
    public void testDefaultsFromEmptyEnvOptions() {
        ObservabilityConfig cfg = ObservabilityConfig.fromEnvOptions(Collections.emptyMap());
        Assertions.assertFalse(cfg.isEnabled());
        Assertions.assertEquals(5000L, cfg.getBucketMs());
        Assertions.assertEquals(3, cfg.getRetentionMinutes());
        Assertions.assertEquals(Collections.emptyList(), cfg.getAsyncBoundaries());
        Assertions.assertEquals(0, cfg.getEdgeBufferCapacity());
        Assertions.assertEquals(Collections.emptyMap(), cfg.getEdgeOverrides());
        Assertions.assertFalse(cfg.isSplitSinkIo());
    }

    @Test
    public void testClampBucketAndRetention() {
        Map<String, Object> env = new HashMap<>();
        env.put("engine.observability.enabled", true);
        env.put("engine.observability.bucket_ms", 500);
        env.put("engine.observability.retention_minutes", 0);
        ObservabilityConfig cfg = ObservabilityConfig.fromEnvOptions(env);
        Assertions.assertTrue(cfg.isEnabled());
        Assertions.assertEquals(1000L, cfg.getBucketMs());
        Assertions.assertEquals(1, cfg.getRetentionMinutes());
    }

    @Test
    public void testAutoEnableWhenAsyncBoundariesConfiguredAndEnabledNotSet() {
        Map<String, Object> env = new HashMap<>();
        env.put("engine.observability.async_boundaries", Collections.singletonList("t_sql"));
        ObservabilityConfig cfg = ObservabilityConfig.fromEnvOptions(env);
        Assertions.assertTrue(cfg.isEnabled());
    }

    @Test
    public void testDoNotAutoEnableWhenAsyncBoundariesConfiguredButExplicitlyDisabled() {
        Map<String, Object> env = new HashMap<>();
        env.put("engine.observability.enabled", false);
        env.put("engine.observability.async_boundaries", Collections.singletonList("t_sql"));
        ObservabilityConfig cfg = ObservabilityConfig.fromEnvOptions(env);
        Assertions.assertFalse(cfg.isEnabled());
    }

    @Test
    public void testAutoEnableWhenSplitSinkIoConfiguredAndEnabledNotSet() {
        Map<String, Object> env = new HashMap<>();
        env.put("engine.observability.split_sink_io", true);
        ObservabilityConfig cfg = ObservabilityConfig.fromEnvOptions(env);
        Assertions.assertTrue(cfg.isEnabled());
        Assertions.assertTrue(cfg.isSplitSinkIo());
    }

    @Test
    public void testDoNotAutoEnableWhenSplitSinkIoConfiguredButExplicitlyDisabled() {
        Map<String, Object> env = new HashMap<>();
        env.put("engine.observability.enabled", false);
        env.put("engine.observability.split_sink_io", true);
        ObservabilityConfig cfg = ObservabilityConfig.fromEnvOptions(env);
        Assertions.assertFalse(cfg.isEnabled());
        Assertions.assertTrue(cfg.isSplitSinkIo());
    }

    @Test
    public void testNestedMapParsing() {
        Map<String, Object> observability = new HashMap<>();
        observability.put("enabled", true);
        observability.put("bucket_ms", 7000L);
        observability.put("retention_minutes", 30);
        observability.put("async_boundaries", Collections.singletonList("t_replace"));
        observability.put("edge_buffer_capacity", 123);
        observability.put("split_sink_io", true);

        Map<String, Object> engine = new HashMap<>();
        engine.put("observability", observability);

        Map<String, Object> env = new HashMap<>();
        env.put("engine", engine);

        ObservabilityConfig cfg = ObservabilityConfig.fromEnvOptions(env);
        Assertions.assertTrue(cfg.isEnabled());
        Assertions.assertEquals(7000L, cfg.getBucketMs());
        Assertions.assertEquals(10, cfg.getRetentionMinutes());
        Assertions.assertEquals(Collections.singletonList("t_replace"), cfg.getAsyncBoundaries());
        Assertions.assertEquals(123, cfg.getEdgeBufferCapacity());
        Assertions.assertTrue(cfg.isSplitSinkIo());
    }

    @Test
    public void testMaxRetentionCap() {
        Map<String, Object> env = new HashMap<>();
        env.put("engine.observability.enabled", true);
        env.put("engine.observability.retention_minutes", 999);
        ObservabilityConfig cfg = ObservabilityConfig.fromEnvOptions(env);
        Assertions.assertEquals(10, cfg.getRetentionMinutes());
    }

    @Test
    public void testEdgeOverridesParsingAndCapacityResolution() {
        List<Map<String, Object>> overrides = new ArrayList<>();
        overrides.add(override("a", 10));
        overrides.add(override("b", "20"));
        overrides.add(override("huge", 999999999));
        overrides.add(override("bad", "NaN"));
        overrides.add(Collections.singletonMap("boundary", "missingCapacity"));

        Map<String, Object> env = new HashMap<>();
        env.put("engine.observability.enabled", true);
        env.put("engine.observability.edge_buffer_capacity", 7);
        env.put("engine.observability.edge_overrides", overrides);

        ObservabilityConfig cfg = ObservabilityConfig.fromEnvOptions(env);
        Assertions.assertEquals(7, cfg.capacityForBoundary(null));
        Assertions.assertEquals(10, cfg.capacityForBoundary("a"));
        Assertions.assertEquals(20, cfg.capacityForBoundary("b"));
        Assertions.assertEquals(100_000, cfg.capacityForBoundary("huge"));
        Assertions.assertEquals(7, cfg.capacityForBoundary("bad"));
        Assertions.assertEquals(7, cfg.capacityForBoundary("missingCapacity"));
    }

    private static Map<String, Object> override(String boundary, Object capacity) {
        Map<String, Object> m = new HashMap<>();
        m.put("boundary", boundary);
        m.put("capacity", capacity);
        return m;
    }

    @Test
    public void testGetEnabledOrNull() {
        Assertions.assertNull(ObservabilityConfig.getEnabledOrNull(Collections.emptyMap()));

        Map<String, Object> envFalse = new HashMap<>();
        envFalse.put("engine.observability.enabled", false);
        Assertions.assertEquals(Boolean.FALSE, ObservabilityConfig.getEnabledOrNull(envFalse));

        Map<String, Object> envTrue = new HashMap<>();
        envTrue.put("engine.observability.enabled", "true");
        Assertions.assertEquals(Boolean.TRUE, ObservabilityConfig.getEnabledOrNull(envTrue));
    }
}

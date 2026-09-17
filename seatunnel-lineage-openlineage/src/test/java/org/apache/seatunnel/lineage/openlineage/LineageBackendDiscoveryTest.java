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

package org.apache.seatunnel.lineage.openlineage;

import org.apache.seatunnel.lineage.LineageBackend;
import org.apache.seatunnel.lineage.LineageBackendLoader;
import org.apache.seatunnel.lineage.LineageConfig;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the packaging contract rather than the logic.
 *
 * <p>The reporter reaches its transport through {@link ServiceLoader}. A module that depends only
 * on the lineage contract compiles and runs, but its shaded jar carries no provider file, so the
 * backend cannot be resolved and every event is dropped with a warning. That is invisible to any
 * test that constructs the backend directly, so the lookup is asserted the way production performs
 * it.
 */
class LineageBackendDiscoveryTest {

    @Test
    void httpBackendIsReachableThroughTheServiceLoader() {
        boolean found = false;
        for (LineageBackend backend : ServiceLoader.load(LineageBackend.class)) {
            if ("http".equals(backend.getName())) {
                found = true;
            }
        }
        assertTrue(
                found,
                "no LineageBackend named 'http' on the classpath: the provider file is missing, "
                        + "which means a packaging or dependency change dropped "
                        + "seatunnel-lineage-openlineage");
    }

    @Test
    void enabledConfigResolvesTheHttpBackendRatherThanFailing() {
        Map<String, Object> options = new HashMap<>();
        options.put(LineageConfig.ENABLED, true);
        options.put(LineageConfig.URL, "http://127.0.0.1:1/api/lineage");
        LineageConfig config =
                LineageConfig.resolve(options, Collections.emptyMap(), Collections.emptyMap());

        LineageBackend backend = LineageBackendLoader.load(config);

        assertNotNull(backend);
        assertTrue(
                backend instanceof OpenLineageBackend,
                "the default transport must resolve to the OpenLineage backend, not fail");
    }
}

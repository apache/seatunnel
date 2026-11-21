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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.aliyun.odps.Odps;

import java.util.HashMap;
import java.util.Map;

public class MaxComputeCatalogTest {

    /** Minimal config that lets the catalog build an Odps client without network calls. */
    private static Map<String, Object> baseConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("accessId", "my-id");
        config.put("accesskey", "my-key");
        config.put("endpoint", "http://service.odps.aliyun.com/api");
        config.put("project", "my_project");
        return config;
    }

    /**
     * The REST client timeout/retry options must be applied to the Odps instance built by
     * MaxComputeCatalog.getOdps, so that metadata and DDL calls honor connect_timeout_ms /
     * read_timeout_ms / retry_times. This guards against the catalog path silently ignoring the
     * options (the most common metadata path: source schema discovery and sink save-mode DDL).
     */
    @Test
    void testCatalogAppliesRestClientOptions() {
        Map<String, Object> config = baseConfig();
        config.put("connect_timeout_ms", 30000L);
        config.put("read_timeout_ms", 60000L);
        config.put("retry_times", 7);

        MaxComputeCatalog catalog = new MaxComputeCatalog("test", ReadonlyConfig.fromMap(config));
        catalog.open();

        Odps odps = catalog.getOdps("my_project", null);
        Assertions.assertEquals(30, odps.getRestClient().getConnectTimeout());
        Assertions.assertEquals(60, odps.getRestClient().getReadTimeout());
        Assertions.assertEquals(7, odps.getRestClient().getRetryTimes());
    }

    /**
     * When no REST timeout/retry options are supplied, the catalog must fall back to the option
     * defaults (connect 10s, read 120s, retry 4) — i.e. the original SDK behavior is preserved.
     */
    @Test
    void testCatalogUsesRestClientDefaultsWhenOptionsAbsent() {
        MaxComputeCatalog catalog =
                new MaxComputeCatalog("test", ReadonlyConfig.fromMap(baseConfig()));
        catalog.open();

        Odps odps = catalog.getOdps("my_project", null);
        Assertions.assertEquals(10, odps.getRestClient().getConnectTimeout());
        Assertions.assertEquals(120, odps.getRestClient().getReadTimeout());
        Assertions.assertEquals(4, odps.getRestClient().getRetryTimes());
    }
}

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

package org.apache.seatunnel.engine.common.config;

import java.util.Map;

/** Internal job options used by the local sample dry-run path. */
public final class DryRunSampleConfig {

    public static final int DEFAULT_LIMIT = 10;

    private static final String ENABLED_KEY = "__seatunnel_dry_run_sample";
    private static final String LIMIT_KEY = "__seatunnel_dry_run_sample_limit";

    private DryRunSampleConfig() {}

    public static void configure(Map<String, Object> envOptions, int limit) {
        envOptions.put(ENABLED_KEY, true);
        envOptions.put(LIMIT_KEY, limit);
    }

    public static boolean isEnabled(Map<String, Object> envOptions) {
        return Boolean.TRUE.equals(envOptions.get(ENABLED_KEY));
    }

    public static int getLimit(Map<String, Object> envOptions) {
        Object limit = envOptions.get(LIMIT_KEY);
        return limit instanceof Number ? ((Number) limit).intValue() : DEFAULT_LIMIT;
    }
}

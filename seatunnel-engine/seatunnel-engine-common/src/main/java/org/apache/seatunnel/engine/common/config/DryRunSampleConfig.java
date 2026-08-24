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
    public static final int MAX_LIMIT = 10_000;

    private static final String ENABLED_KEY = "__seatunnel_dry_run_sample";
    private static final String LIMIT_KEY = "__seatunnel_dry_run_sample_limit";
    private static final String PRINT_DATA_KEY = "__seatunnel_dry_run_sample_print_data";

    private DryRunSampleConfig() {}

    /** Records a sample request from the validated local CLI without exposing it as job config. */
    public static void configure(JobConfig jobConfig, int limit, boolean printData) {
        jobConfig.setDryRunSample(true);
        jobConfig.setDryRunSampleLimit(limit);
        jobConfig.setDryRunSamplePrintData(printData);
    }

    /** Removes user-provided internal keys and publishes only the trusted CLI request to tasks. */
    public static void applyTrustedConfiguration(JobConfig jobConfig) {
        Map<String, Object> envOptions = jobConfig.getEnvOptions();
        envOptions.remove(ENABLED_KEY);
        envOptions.remove(LIMIT_KEY);
        envOptions.remove(PRINT_DATA_KEY);
        if (jobConfig.isDryRunSample()) {
            configureRuntimeOptions(
                    envOptions,
                    jobConfig.getDryRunSampleLimit(),
                    jobConfig.isDryRunSamplePrintData());
        }
    }

    private static void configureRuntimeOptions(
            Map<String, Object> envOptions, int limit, boolean printData) {
        envOptions.put(ENABLED_KEY, true);
        envOptions.put(LIMIT_KEY, limit);
        envOptions.put(PRINT_DATA_KEY, printData);
    }

    /** Returns whether local sample dry-run mode is enabled for the task. */
    public static boolean isEnabled(Map<String, Object> envOptions) {
        return Boolean.TRUE.equals(envOptions.get(ENABLED_KEY));
    }

    /** Returns the configured sample row limit, or the default limit when it is absent. */
    public static int getLimit(Map<String, Object> envOptions) {
        Object limit = envOptions.get(LIMIT_KEY);
        return limit instanceof Number ? ((Number) limit).intValue() : DEFAULT_LIMIT;
    }

    /** Returns whether sampled row values should be written to persistent logs. */
    public static boolean isPrintData(Map<String, Object> envOptions) {
        return Boolean.TRUE.equals(envOptions.get(PRINT_DATA_KEY));
    }
}

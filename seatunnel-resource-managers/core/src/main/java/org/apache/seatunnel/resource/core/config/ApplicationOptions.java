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

package org.apache.seatunnel.resource.core.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/** Options shared by external application deployment targets. */
public final class ApplicationOptions {
    private ApplicationOptions() {}

    public static final Option<String> NAME =
            Options.key("application.name")
                    .stringType()
                    .defaultValue("seatunnel")
                    .withDescription("Application display name.");

    public static final Option<Long> JOB_ID =
            Options.key("application.job-id")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Positive native Zeta job ID. Generated once before deployment when omitted.");

    public static final Option<Long> RESTORE_JOB_ID =
            Options.key("application.restore-job-id")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Historical Zeta job ID whose latest eligible checkpoint restores this new job.");

    public static final Option<Integer> WORKER_COUNT =
            Options.key("application.worker-count")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Fixed number of workers provisioned before job submission.");

    public static final Option<Integer> WORKER_MEMORY_MB =
            Options.key("application.worker.memory-mb")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("Total memory in MiB requested for each worker container.");

    public static final Option<Integer> WORKER_CPU_CORES =
            Options.key("application.worker.cpu-cores")
                    .intType()
                    .defaultValue(1)
                    .withDescription("CPU cores requested for each worker.");

    public static final Option<Integer> WORKER_SLOTS =
            Options.key("application.worker.slots")
                    .intType()
                    .defaultValue(2)
                    .withDescription("Fixed execution slots on each worker.");

    public static final Option<Long> STARTUP_TIMEOUT_MILLIS =
            Options.key("application.startup-timeout-millis")
                    .longType()
                    .defaultValue(120000L)
                    .withDescription(
                            "Timeout in milliseconds for each of master startup and worker provisioning and registration.");

    public static final Option<Integer> MASTER_MEMORY_MB =
            Options.key("application.master.memory-mb")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("Total memory in MiB requested for the application master.");

    public static final Option<Integer> MASTER_CPU_CORES =
            Options.key("application.master.cpu-cores")
                    .intType()
                    .defaultValue(1)
                    .withDescription("CPU cores requested for the application master.");

    public static final Option<Integer> MASTER_PORT =
            Options.key("application.master.port")
                    .intType()
                    .defaultValue(5801)
                    .withDescription("Hazelcast port advertised by the application master.");
}

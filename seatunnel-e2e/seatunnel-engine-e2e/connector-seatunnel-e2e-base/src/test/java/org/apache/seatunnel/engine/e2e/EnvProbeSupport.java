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

package org.apache.seatunnel.engine.e2e;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.EnvCommonOptions;

import java.util.Map;

/** Shared helper that renders the whole env block into one probe string. */
final class EnvProbeSupport {

    static final Option<String> PROBE_OPTION = Options.key("probe").stringType().noDefaultValue();

    private EnvProbeSupport() {}

    /**
     * Reads one engine-declared key ({@code job.mode}), one user-defined top-level key ({@code
     * probe}) and one nested {@code custom_parameters} key, proving the entire env block is
     * forwarded, not only a single namespace.
     */
    static String probe(ReadonlyConfig envOptions) {
        String jobMode = String.valueOf(envOptions.get(EnvCommonOptions.JOB_MODE));
        String probe = envOptions.get(PROBE_OPTION);
        Map<String, String> customParameters = envOptions.get(EnvCommonOptions.CUSTOM_PARAMETERS);
        String nested = customParameters == null ? null : customParameters.get("nested");
        return jobMode + "|" + probe + "|" + nested;
    }
}

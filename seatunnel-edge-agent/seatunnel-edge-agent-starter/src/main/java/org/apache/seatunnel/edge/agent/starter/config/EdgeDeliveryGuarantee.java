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

package org.apache.seatunnel.edge.agent.starter.config;

import java.util.Locale;
import java.util.Objects;

/** Outbound delivery mode configured on the agent (validation only in this release). */
public enum EdgeDeliveryGuarantee {
    BEST_EFFORT;

    public static EdgeDeliveryGuarantee from(String value) {
        if (value == null || value.trim().isEmpty()) {
            return BEST_EFFORT;
        }
        String normalized = value.trim().replace('-', '_').toUpperCase(Locale.ROOT);
        if (Objects.equals(BEST_EFFORT.name(), normalized)) {
            return BEST_EFFORT;
        }
        throw new IllegalArgumentException(
                "Unsupported agent.delivery-guarantee: "
                        + value
                        + ". Supported in this release: BEST_EFFORT (aliases: best-effort,"
                        + " best_effort).");
    }

    public static void validateSupported(String value) {
        from(value);
    }
}

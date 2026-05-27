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

/** Outbound delivery mode configured on the agent. */
public enum EdgeDeliveryGuarantee {
    BEST_EFFORT("sqlite"),
    NON("mem");

    private final String storeFactoryId;

    EdgeDeliveryGuarantee(String storeFactoryId) {
        this.storeFactoryId = storeFactoryId;
    }

    /**
     * Returns the SPI factory identifier used to discover the matching {@link
     * org.apache.seatunnel.edge.agent.starter.wal.WalStoreFactory}.
     */
    public String storeFactoryId() {
        return storeFactoryId;
    }

    public static EdgeDeliveryGuarantee from(String value) {
        if (value == null || value.trim().isEmpty()) {
            return BEST_EFFORT;
        }
        String normalized = value.trim().replace('-', '_').toUpperCase(Locale.ROOT);
        if (Objects.equals(BEST_EFFORT.name(), normalized)) {
            return BEST_EFFORT;
        }
        if (Objects.equals(NON.name(), normalized) || Objects.equals("NONE", normalized)) {
            return NON;
        }
        throw new IllegalArgumentException(
                "Unsupported agent.delivery-guarantee: "
                        + value
                        + ". Supported: BEST_EFFORT (aliases: best-effort, best_effort),"
                        + " NON (aliases: non, none).");
    }

    public static void validateSupported(String value) {
        from(value);
    }
}

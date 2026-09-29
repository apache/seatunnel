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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize;

import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

public enum EdgeSocketPacketMode {
    RAW("raw"),
    PACKET("packet");

    private final String value;

    EdgeSocketPacketMode(String value) {
        this.value = value;
    }

    /** @return canonical option value used in connector config */
    public String getValue() {
        return value;
    }

    /**
     * Resolve packet mode from config value.
     *
     * @param mode configured packet mode string
     * @return matched packet mode enum
     */
    public static EdgeSocketPacketMode from(String mode) {
        Objects.requireNonNull(mode, "packetMode must not be null");
        String normalized = mode.trim().toLowerCase(Locale.ROOT);
        for (EdgeSocketPacketMode packetMode : values()) {
            if (packetMode.getValue().equals(normalized)
                    || packetMode.name().equalsIgnoreCase(normalized)) {
                return packetMode;
            }
        }
        String supported =
                Arrays.stream(values())
                        .map(EdgeSocketPacketMode::getValue)
                        .collect(Collectors.joining(", "));
        throw new IllegalArgumentException(
                "Unsupported packet mode: " + mode + ", supported: " + supported);
    }
}

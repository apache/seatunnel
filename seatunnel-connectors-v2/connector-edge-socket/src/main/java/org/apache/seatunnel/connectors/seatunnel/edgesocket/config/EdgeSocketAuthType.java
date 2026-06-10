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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.config;

import lombok.Getter;

import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

@Getter
public enum EdgeSocketAuthType {
    TOKEN("token");

    private final String value;

    EdgeSocketAuthType(String value) {
        this.value = value;
    }

    public static EdgeSocketAuthType from(String authType) {
        Objects.requireNonNull(authType, "authType must not be null");
        String normalized = authType.trim().toLowerCase(Locale.ROOT);
        for (EdgeSocketAuthType type : values()) {
            if (type.getValue().equals(normalized) || type.name().equalsIgnoreCase(normalized)) {
                return type;
            }
        }
        String supported =
                Arrays.stream(values())
                        .map(EdgeSocketAuthType::getValue)
                        .collect(Collectors.joining(", "));
        throw new IllegalArgumentException(
                "Unsupported auth type: " + authType + ", supported: " + supported);
    }
}

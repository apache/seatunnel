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

public enum EdgeSocketCompressionType {
    NONE("none"),
    GZIP("gzip"),
    ZLIB("zlib"),
    DEFLATE("deflate");

    private final String value;

    EdgeSocketCompressionType(String value) {
        this.value = value;
    }

    /** @return canonical option value used in configs and packet fields */
    public String getValue() {
        return value;
    }

    /**
     * Resolve compression enum from config/packet value.
     *
     * @param value user configured or packet declared compression string
     * @return matched compression type
     */
    public static EdgeSocketCompressionType from(String value) {
        Objects.requireNonNull(value, "compressionType must not be null");
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        for (EdgeSocketCompressionType compressionType : values()) {
            if (compressionType.getValue().equals(normalized)
                    || compressionType.name().equalsIgnoreCase(normalized)) {
                return compressionType;
            }
        }
        String supported =
                Arrays.stream(values())
                        .map(EdgeSocketCompressionType::getValue)
                        .collect(Collectors.joining(", "));
        throw new IllegalArgumentException(
                "Unsupported compression type: " + value + ", supported: " + supported);
    }
}

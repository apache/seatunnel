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

package org.apache.seatunnel.api.cdc;

import org.apache.seatunnel.api.annotation.Experimental;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Connector-native CDC position with an explicit position family and schema version.
 *
 * <p>The map keys are defined by the position family and schema version. Consumers should not
 * assume that keys from one connector family apply to another. Values are copied at construction
 * time and exposed as an unmodifiable map. Position values must contain only offset coordinates;
 * credentials, connection URLs, and other authentication material are forbidden.
 */
@Experimental
public final class CdcProgressPosition {

    /** Position family, for example {@code MYSQL_BINLOG}. */
    private final String type;

    /** Schema version for interpreting {@link #values}. */
    private final int schemaVersion;

    /** Connector-native position fields represented as stable string values. */
    private final Map<String, String> values;

    public CdcProgressPosition(String type, int schemaVersion, Map<String, String> values) {
        this.type = Objects.requireNonNull(type, "type must not be null");
        if (schemaVersion < 1) {
            throw new IllegalArgumentException("schemaVersion must be positive");
        }
        this.schemaVersion = schemaVersion;
        this.values =
                Collections.unmodifiableMap(
                        new LinkedHashMap<>(
                                Objects.requireNonNull(values, "values must not be null")));
    }

    public String getType() {
        return type;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public Map<String, String> getValues() {
        return values;
    }
}

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

package org.apache.seatunnel.api.configuration;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Option<T> {
    /** The current key for that config option. */
    private final String key;

    /** Type of the value that this Option describes. */
    private final TypeReference<T> typeReference;

    /** The default value for this option. */
    private final T defaultValue;

    /** The description for this option. */
    String description = "";

    @Getter private final List<String> fallbackKeys;

    public Option(String key, TypeReference<T> typeReference, T defaultValue) {
        this.key = key;
        this.typeReference = typeReference;
        this.defaultValue = defaultValue;
        this.fallbackKeys = new ArrayList<>();
    }

    public String key() {
        return key;
    }

    public TypeReference<T> typeReference() {
        return typeReference;
    }

    public T defaultValue() {
        return defaultValue;
    }

    public String getDescription() {
        return description;
    }

    public Option<T> withDescription(String description) {
        this.description = description;
        return this;
    }

    public Option<T> withFallbackKeys(String... fallbackKeys) {
        this.fallbackKeys.addAll(Arrays.asList(fallbackKeys));
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Option)) {
            return false;
        }
        Option<?> that = (Option<?>) obj;
        return Objects.equals(this.key, that.key)
                && Objects.equals(this.defaultValue, that.defaultValue)
                && Objects.equals(this.fallbackKeys, that.fallbackKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.key, this.defaultValue, this.fallbackKeys);
    }

    @Override
    public String toString() {
        return String.format(
                "Key: '%s', default: %s (fallback keys: %s)", key, defaultValue, fallbackKeys);
    }
}

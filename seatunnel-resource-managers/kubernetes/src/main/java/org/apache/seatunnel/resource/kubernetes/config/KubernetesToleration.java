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

package org.apache.seatunnel.resource.kubernetes.config;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * One Pod toleration configured through {@code kubernetes.master.tolerations} or {@code
 * kubernetes.worker.tolerations}.
 *
 * <p>The option carries {@code key[=value][:Effect]} entries separated by {@code ;}. A configured
 * value selects the {@code Equal} operator, a bare key the {@code Exists} operator; the effect is
 * one of {@code NoSchedule}, {@code PreferNoSchedule} or {@code NoExecute} and matches every effect
 * when it is omitted.
 */
@Getter
public final class KubernetesToleration {
    private static final Set<String> EFFECTS =
            new LinkedHashSet<>(Arrays.asList("NoSchedule", "PreferNoSchedule", "NoExecute"));
    private static final String EQUAL_OPERATOR = "Equal";
    private static final String EXISTS_OPERATOR = "Exists";

    /** Entry separator of the configured list. */
    private static final String ENTRY_SEPARATOR = ";";

    private final String key;
    private final String operator;
    private final String value;
    private final String effect;

    private KubernetesToleration(String key, String operator, String value, String effect) {
        this.key = key;
        this.operator = operator;
        this.value = value;
        this.effect = effect;
    }

    /**
     * Parses the configured entries.
     *
     * @param optionKey option that carries the entries, used in error messages
     * @param configured {@code key[=value][:Effect]} entries separated by {@code ;}, or empty
     * @return parsed tolerations in configuration order
     * @throws IllegalArgumentException if an entry is empty or malformed
     */
    public static List<KubernetesToleration> parse(String optionKey, String configured) {
        if (configured == null || configured.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<KubernetesToleration> tolerations = new ArrayList<>();
        for (String entry : configured.split(ENTRY_SEPARATOR)) {
            String text = entry.trim();
            if (text.isEmpty()) {
                throw new IllegalArgumentException(
                        optionKey + " contains an empty toleration entry");
            }
            tolerations.add(parseEntry(optionKey, text));
        }
        return tolerations;
    }

    /** Returns the entries in the syntax {@link #parse(String, String)} accepts. */
    public static String serialize(List<KubernetesToleration> tolerations) {
        return tolerations.stream()
                .map(KubernetesToleration::serialize)
                .collect(Collectors.joining(ENTRY_SEPARATOR));
    }

    private static KubernetesToleration parseEntry(String optionKey, String text) {
        String key = text;
        String effect = "";
        // Kubernetes accepts only the three effects and requires toleration values to be valid
        // label values, so a ':' segment that is not an effect is rejected instead of silently
        // becoming part of the value.
        int effectSeparator = key.indexOf(':');
        if (effectSeparator >= 0) {
            effect = key.substring(effectSeparator + 1).trim();
            key = key.substring(0, effectSeparator).trim();
            if (!EFFECTS.contains(effect)) {
                throw new IllegalArgumentException(
                        optionKey + " effect " + effect + " must be one of " + EFFECTS);
            }
        }
        String value = "";
        int valueSeparator = key.indexOf('=');
        if (valueSeparator >= 0) {
            value = key.substring(valueSeparator + 1).trim();
            key = key.substring(0, valueSeparator).trim();
            if (value.isEmpty()) {
                throw new IllegalArgumentException(
                        optionKey + " toleration " + text + " must declare a value after '='");
            }
        }
        if (key.isEmpty()) {
            throw new IllegalArgumentException(
                    optionKey + " toleration " + text + " must declare a key");
        }
        String operator = EQUAL_OPERATOR;
        if (value.isEmpty()) {
            operator = EXISTS_OPERATOR;
        }
        return new KubernetesToleration(key, operator, value, effect);
    }

    private String serialize() {
        StringBuilder text = new StringBuilder(key);
        if (!value.isEmpty()) {
            text.append('=').append(value);
        }
        if (!effect.isEmpty()) {
            text.append(':').append(effect);
        }
        return text.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof KubernetesToleration)) {
            return false;
        }
        KubernetesToleration that = (KubernetesToleration) other;
        return key.equals(that.key)
                && operator.equals(that.operator)
                && value.equals(that.value)
                && effect.equals(that.effect);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, operator, value, effect);
    }

    @Override
    public String toString() {
        return serialize();
    }
}

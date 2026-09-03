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

package org.apache.seatunnel.core.starter.validation;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Versioned, config-level validation result shared by CLI and future adapters.
 *
 * <p>This model intentionally describes static/config validation only. It does not imply that a
 * connector can reach its external system or that a generated job is runtime-equivalent.
 */
public final class ConfigValidationResult implements Serializable {

    private static final long serialVersionUID = 1L;
    public static final String SCHEMA_VERSION = "1.0";

    private final boolean valid;
    private final String phase;
    private final List<ConfigValidationError> errors;

    private ConfigValidationResult(
            boolean valid, String phase, List<ConfigValidationError> errors) {
        this.valid = valid;
        this.phase = phase;
        this.errors = Collections.unmodifiableList(new ArrayList<>(errors));
    }

    public static ConfigValidationResult success(String phase) {
        return new ConfigValidationResult(true, phase, Collections.emptyList());
    }

    public static ConfigValidationResult failure(
            String phase, ConfigValidationError error) {
        return new ConfigValidationResult(false, phase, Collections.singletonList(error));
    }

    public boolean isValid() {
        return valid;
    }

    public String getPhase() {
        return phase;
    }

    public List<ConfigValidationError> getErrors() {
        return errors;
    }

    /** Serialize fields in a fixed order so adapters can rely on a stable shape. */
    public String toJson() {
        ObjectNode root = JsonUtils.createObjectNode();
        root.put("schemaVersion", SCHEMA_VERSION);
        root.put("valid", valid);
        root.put("phase", phase);
        ArrayNode errorNodes = root.putArray("errors");
        for (ConfigValidationError error : errors) {
            ObjectNode errorNode = errorNodes.addObject();
            putNullable(errorNode, "location", error.getLocation());
            putNullable(errorNode, "plugin", error.getPlugin());
            putNullable(errorNode, "optionPath", error.getOptionPath());
            putNullable(errorNode, "ruleCategory", error.getRuleCategory());
            putNullable(errorNode, "message", error.getMessage());
        }
        return root.toString();
    }

    /** Preserve the existing --check message for the CLI adapter. */
    public String toHumanReadable() {
        if (valid) {
            return "VALID";
        }
        String message = errors.isEmpty() ? "Validation failed" : errors.get(0).getMessage();
        return humanPhase(phase) + " failed: " + message;
    }

    private static String humanPhase(String phase) {
        if ("connectivity".equals(phase)) {
            return "Connectivity check";
        }
        if ("static".equals(phase)) {
            return "Static analysis";
        }
        return phase;
    }

    private static void putNullable(ObjectNode node, String name, String value) {
        if (value == null) {
            node.putNull(name);
        } else {
            node.put(name, value);
        }
    }
}

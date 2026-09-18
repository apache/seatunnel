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

import java.io.Serializable;

/** A machine-readable validation failure at config level. */
public final class ConfigValidationError implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Plugin location, such as {@code source[0](FakeSource)}, when the phase can identify it. */
    private final String location;

    /** Plugin factory identifier parsed from {@link #location}, when available. */
    private final String plugin;

    /** Option or option group reported by the underlying option validator, when available. */
    private final String optionPath;

    /** Stable, closed category describing the validation rule that failed. */
    private final String ruleCategory;

    /** Sanitized diagnostic message suitable for programmatic consumers. */
    private final String message;

    public ConfigValidationError(
            String location,
            String plugin,
            String optionPath,
            String ruleCategory,
            String message) {
        this.location = location;
        this.plugin = plugin;
        this.optionPath = optionPath;
        this.ruleCategory = ruleCategory;
        this.message = message;
    }

    public String getLocation() {
        return location;
    }

    public String getPlugin() {
        return plugin;
    }

    public String getOptionPath() {
        return optionPath;
    }

    public String getRuleCategory() {
        return ruleCategory;
    }

    public String getMessage() {
        return message;
    }
}

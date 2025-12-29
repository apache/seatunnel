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
package org.apache.seatunnel.api.configuration.util.issue;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.common.constants.PluginType;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeprecatedConfigurationIssue extends ConfigurationVerificationIssue {

    private final Option<?>[] referToOptions;

    private DeprecatedConfigurationIssue(
            String identifier,
            PluginType pluginType,
            Option<?> option,
            Option<?>[] referToOptions) {
        super(Level.WARNING, identifier, pluginType, option);
        this.referToOptions = referToOptions;
    }

    @Override
    public void log() {
        if (referToOptions == null || referToOptions.length == 0) {
            log.warn(
                    "Deprecated configuration option '{}' detected in {} plugin '{}'",
                    option.key(),
                    pluginType.getType(),
                    identifier);
            return;
        }
        StringBuilder suggestionMessage = new StringBuilder();
        for (int i = 0; i < referToOptions.length; i++) {
            if (i > 0) {
                suggestionMessage.append(", ");
            }
            suggestionMessage.append(referToOptions[i].key());
        }
        log.warn(
                "Deprecated configuration option '{}' detected in {} plugin '{}', please refer to {}",
                option.key(),
                pluginType.getType(),
                identifier,
                suggestionMessage);
    }

    public static DeprecatedConfigurationIssue of(
            String identifier,
            PluginType pluginType,
            Option<?> option,
            Option<?>[] referToOptions) {
        return new DeprecatedConfigurationIssue(identifier, pluginType, option, referToOptions);
    }

    public static DeprecatedConfigurationIssue of(
            String identifier, PluginType pluginType, Option<?> option) {
        return new DeprecatedConfigurationIssue(identifier, pluginType, option, null);
    }
}

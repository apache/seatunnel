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
public class ConflictConfigurationIssue extends ConfigurationVerificationIssue {

    private final Option<?> option;
    private final Object value;
    private final Option<?> conflictOption;
    private final Object conflictValue;

    private ConflictConfigurationIssue(
            Level level,
            String identifier,
            PluginType pluginType,
            Option<?> option,
            Object value,
            Option<?> conflictOption,
            Object conflictValue) {
        super(level, identifier, pluginType);
        this.option = option;
        this.value = value;
        this.conflictOption = conflictOption;
        this.conflictValue = conflictValue;
    }

    @Override
    public String getLog() {
        return prefixMessage(
                String.format(
                        "Configuration option '%s' with value '%s' conflicts with option '%s' (value '%s') in %s plugin '%s'",
                        option.key(),
                        value,
                        conflictOption.key(),
                        conflictValue,
                        pluginType.getType(),
                        identifier));
    }

    public static ConflictConfigurationIssue errorOf(
            String identifier,
            PluginType pluginType,
            Option<?> option,
            Object value,
            Option<?> conflictOption,
            Object conflictValue) {
        return new ConflictConfigurationIssue(
                Level.ERROR, identifier, pluginType, option, value, conflictOption, conflictValue);
    }

    public static ConflictConfigurationIssue warnOf(
            String identifier,
            PluginType pluginType,
            Option<?> option,
            Object value,
            Option<?> conflictOption,
            Object conflictValue) {
        return new ConflictConfigurationIssue(
                Level.WARNING,
                identifier,
                pluginType,
                option,
                value,
                conflictOption,
                conflictValue);
    }
}

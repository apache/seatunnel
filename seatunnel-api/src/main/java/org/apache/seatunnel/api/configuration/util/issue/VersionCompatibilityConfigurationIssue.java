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

import java.util.Optional;

@Slf4j
public class VersionCompatibilityConfigurationIssue extends ConfigurationVerificationIssue {

    private final Option<?> option;
    private final String needVersion;
    private final Optional<String> currentVersion;

    private VersionCompatibilityConfigurationIssue(
            Level level,
            String identifier,
            PluginType pluginType,
            Option<?> option,
            String needVersion,
            Optional<String> currentVersion) {
        super(level, identifier, pluginType);
        this.option = option;
        this.needVersion = needVersion;
        this.currentVersion = currentVersion;
    }

    @Override
    public String getLog() {
        String versionInfo =
                currentVersion
                        .map(version -> String.format("current version '%s'", version))
                        .orElse("current version is unknown");
        return prefixMessage(
                String.format(
                        "Configuration option '%s' requires version '%s' (%s) in %s plugin '%s'",
                        option.key(), needVersion, versionInfo, pluginType.getType(), identifier));
    }

    public static VersionCompatibilityConfigurationIssue errorOf(
            String identifier,
            PluginType pluginType,
            Option<?> option,
            String needVersion,
            Optional<String> currentVersion) {
        return new VersionCompatibilityConfigurationIssue(
                Level.ERROR, identifier, pluginType, option, needVersion, currentVersion);
    }

    public static VersionCompatibilityConfigurationIssue warnOf(
            String identifier,
            PluginType pluginType,
            Option<?> option,
            String needVersion,
            Optional<String> currentVersion) {
        return new VersionCompatibilityConfigurationIssue(
                Level.WARNING, identifier, pluginType, option, needVersion, currentVersion);
    }
}

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
package org.apache.seatunnel.api.configuration.util;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.issue.ConfigurationVerificationIssue;
import org.apache.seatunnel.common.constants.PluginType;

import lombok.AllArgsConstructor;

import java.util.Collections;
import java.util.List;

@AllArgsConstructor
public abstract class DefaultEnhancedConfigurationValidator
        implements EnhancedConfigurationValidator {

    protected final String identifier;
    protected final PluginType pluginType;

    @Override
    public List<ConfigurationVerificationIssue> validateDeprecatedRules(ReadonlyConfig context) {
        final List<Option<?>> deprecateOptions = deprecatedOptions(context);
        if (deprecateOptions.isEmpty()) {
            return Collections.emptyList();
        }
        deprecateOptions.forEach(option -> {});
    }

    protected abstract List<Option<?>> deprecatedOptions(ReadonlyConfig context);

    @Override
    public List<ConfigurationVerificationIssue> validateConflictRules(ReadonlyConfig context) {}

    @Override
    public List<ConfigurationVerificationIssue> validateVersionCompatibilityRules(
            ReadonlyConfig context) {}
}

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
package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.DefaultEnhancedConfigurationValidator;
import org.apache.seatunnel.common.constants.PluginType;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions.FIELD_IDE;

@Slf4j
public class JdbcSinkEnhancedValidator extends DefaultEnhancedConfigurationValidator {
    public JdbcSinkEnhancedValidator(String identifier) {
        super(identifier, PluginType.SINK);
    }

    @Override
    protected List<DeprecatedOption> deprecatedOptions(ReadonlyConfig context) {
        List<DeprecatedOption> deprecatedOptions = new ArrayList<>();
        deprecatedOptions.add(DeprecatedOption.warning(FIELD_IDE));
        return deprecatedOptions;
    }

    @Override
    protected List<ConflictOption> conflictOptions(ReadonlyConfig context) {
        return Collections.emptyList();
    }

    @Override
    protected List<VersionCompatibilityOption> versionCompatibilityOptions(ReadonlyConfig context) {
        return Collections.emptyList();
    }
}

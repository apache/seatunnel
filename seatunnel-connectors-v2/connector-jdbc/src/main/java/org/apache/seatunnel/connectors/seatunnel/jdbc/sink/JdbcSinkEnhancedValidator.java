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

import org.apache.seatunnel.api.configuration.util.SinkEnhancedConfigurationValidator;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions.URL;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions.AUTO_COMMIT;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions.IS_EXACTLY_ONCE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions.TABLE_PREFIX;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions.TABLE_SUFFIX;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions.USE_COPY_STATEMENT;

@Slf4j
public class JdbcSinkEnhancedValidator extends SinkEnhancedConfigurationValidator {
    public JdbcSinkEnhancedValidator(String identifier) {
        super(identifier, Optional.empty());
    }

    @Override
    protected List<DeprecatedRule> deprecatedRules() {
        List<DeprecatedRule> deprecatedRules = new ArrayList<>();
        deprecatedRules.add(DeprecatedRule.warning(TABLE_PREFIX));
        deprecatedRules.add(DeprecatedRule.warning(TABLE_SUFFIX));
        return deprecatedRules;
    }

    @Override
    protected List<ConflictRule> conflictRules() {
        List<ConflictRule> conflictRules = new ArrayList<>();
        // check use_copy_statement, error
        conflictRules.add(
                ConflictRule.error(
                        URL,
                        (url, useCopy) ->
                                Boolean.TRUE.equals(useCopy) && !isPostgresFamily(url.toString()),
                        USE_COPY_STATEMENT));
        // check is_exactly_once, warn
        conflictRules.add(
                ConflictRule.warning(
                        IS_EXACTLY_ONCE,
                        (isExactlyOnce, autoCommit) ->
                                Boolean.TRUE.equals(isExactlyOnce)
                                        && Boolean.TRUE.equals(autoCommit),
                        AUTO_COMMIT));
        return conflictRules;
    }

    private boolean isPostgresFamily(String url) {
        return url.startsWith("jdbc:postgresql:") || url.startsWith("jdbc:pivotal:greenplum:");
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.neo4j.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;

public final class Neo4jAuthenticationConditions {

    public static final ConditionExtension<String> AUTHENTICATION_METHOD =
            new AuthenticationMethodCondition();

    public static final ConditionExtension<String> USERNAME_REQUIRES_PASSWORD =
            new UsernamePasswordCondition();

    private Neo4jAuthenticationConditions() {}

    private static final class AuthenticationMethodCondition implements ConditionExtension<String> {

        @Override
        public String description() {
            return "at least one of 'username', 'bearer_token', or 'kerberos_ticket' must be configured";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, String uri) {
            return config.getOptional(Neo4jBaseOptions.KEY_USERNAME).isPresent()
                    || config.getOptional(Neo4jBaseOptions.KEY_BEARER_TOKEN).isPresent()
                    || config.getOptional(Neo4jBaseOptions.KEY_KERBEROS_TICKET).isPresent();
        }
    }

    private static final class UsernamePasswordCondition implements ConditionExtension<String> {

        @Override
        public String description() {
            return "requires 'password' to be configured when 'username' is configured";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, String username) {
            return config.getOptional(Neo4jBaseOptions.KEY_PASSWORD).isPresent();
        }
    }
}

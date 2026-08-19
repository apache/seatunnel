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
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import java.util.Map;

/**
 * Early validation for JDBC sink {@code table_options}. Delegates to {@link
 * JdbcTableOptionsValidator} so dialect-specific rules are defined on {@link
 * org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect}.
 */
public class JdbcTableOptionsConditionExtension implements ConditionExtension<Map<String, String>> {

    public static final JdbcTableOptionsConditionExtension INSTANCE =
            new JdbcTableOptionsConditionExtension();

    private JdbcTableOptionsConditionExtension() {}

    @Override
    public String description() {
        return "must use dialect-specific keys supported by the JDBC sink (see JDBC connector docs)";
    }

    @Override
    public boolean evaluate(ReadonlyConfig config, Map<String, String> value)
            throws OptionValidationException {
        if (value == null || value.isEmpty()) {
            return true;
        }
        try {
            JdbcTableOptionsValidator.validate(config, value);
            return true;
        } catch (JdbcConnectorException e) {
            throw new OptionValidationException(e.getMessage());
        }
    }
}

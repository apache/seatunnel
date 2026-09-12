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
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;

import java.util.Collections;
import java.util.Map;

/** Validates sink {@code table_options} via the resolved {@link JdbcDialect}. */
public final class JdbcTableOptionsValidator {

    private JdbcTableOptionsValidator() {}

    public static void validate(ReadonlyConfig config, Map<String, String> tableOptions) {
        if (tableOptions == null || tableOptions.isEmpty()) {
            return;
        }
        JdbcSinkConfig sinkConfig = JdbcSinkConfig.of(config);
        FieldIdeEnum fieldIdeEnum = config.get(JdbcSinkOptions.FIELD_IDE);
        JdbcDialect dialect =
                JdbcDialectLoader.load(
                        sinkConfig.getJdbcConnectionConfig().getUrl(),
                        sinkConfig.getJdbcConnectionConfig().getCompatibleMode(),
                        sinkConfig.getJdbcConnectionConfig().getDialect(),
                        fieldIdeEnum == null ? null : fieldIdeEnum.getValue());
        dialect.validateTableOptions(tableOptions);
    }

    public static void validate(ReadonlyConfig config) {
        validate(
                config,
                config.getOptional(SinkConnectorCommonOptions.TABLE_OPTIONS)
                        .orElse(Collections.emptyMap()));
    }
}

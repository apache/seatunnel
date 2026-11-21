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
package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.opengauss;

import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresDialectFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psqllow.PostgresLowDialect;

import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;

@AutoService(JdbcDialectFactory.class)
public class OpenGaussDialectFactory extends PostgresDialectFactory {

    @Override
    public String dialectFactoryName() {
        return DatabaseIdentifier.OPENGAUSS;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:opengauss:");
    }

    @Override
    public JdbcDialect create() {
        return new OpenGaussDialect();
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        return new OpenGaussDialect();
    }

    @Override
    public JdbcDialect create(
            @Nonnull String compatibleMode,
            String fieldIde,
            JdbcConnectionConfig connectionConfig) {
        // The runtime overload previously inherited PostgreSQL SQL; retain it rather than the
        // legacy two-argument factory's OpenGauss-specific ON DUPLICATE KEY syntax.
        boolean numericSpecialValueAsNull =
                connectionConfig != null && connectionConfig.isNumericSpecialValueAsNull();
        return createCompatibleDialect(compatibleMode, fieldIde, numericSpecialValueAsNull);
    }

    /**
     * Keeps the three-argument runtime factory's existing PostgreSQL SQL and numeric semantics.
     *
     * <p>This preserves the driver boundary when dialects are selected for runtime execution.
     */
    private static JdbcDialect createCompatibleDialect(
            String compatibleMode, String fieldIde, boolean numericSpecialValueAsNull) {
        if ("postgresLow".equalsIgnoreCase(compatibleMode)) {
            return new PostgresLowDialect(fieldIde, numericSpecialValueAsNull) {
                @Override
                public JdbcRowConverter getRowConverter() {
                    return new OpenGaussJdbcRowConverter(numericSpecialValueAsNull);
                }
            };
        }
        return new PostgresDialect(fieldIde, numericSpecialValueAsNull) {
            @Override
            public JdbcRowConverter getRowConverter() {
                return new OpenGaussJdbcRowConverter(numericSpecialValueAsNull);
            }
        };
    }
}

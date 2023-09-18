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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psqllow.PostgresLowDialect;

import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;

@AutoService(JdbcDialectFactory.class)
public class PostgresDialectFactory implements JdbcDialectFactory {
    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:postgresql:");
    }

    @Override
    public JdbcDialect create() {
        throw new UnsupportedOperationException(
                "Can't create JdbcDialect without compatible mode for Postgres");
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        if ("postgresLow".equalsIgnoreCase(compatibleMode)) {
            return new PostgresLowDialect(fieldIde);
        }
        return new PostgresDialect(fieldIde);
    }
}

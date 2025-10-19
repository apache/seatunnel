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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.duckdb;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectFactory;

import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;

/**
 * Factory for creating {@link DuckDBDialect} instances.
 *
 * <p>This factory is automatically registered via Java SPI (Service Provider Interface) using the
 * AutoService annotation. It creates DuckDB dialect instances when JDBC URLs match the DuckDB
 * pattern.
 */
@AutoService(JdbcDialectFactory.class)
public class DuckDBDialectFactory implements JdbcDialectFactory {

    /**
     * Get the factory name identifier.
     *
     * @return the database identifier for DuckDB
     */
    @Override
    public String dialectFactoryName() {
        return DatabaseIdentifier.DUCKDB;
    }

    /**
     * Check if this factory accepts the given JDBC URL.
     *
     * <p>Accepts URLs starting with "jdbc:duckdb:" prefix.
     *
     * @param url the JDBC URL to check
     * @return true if URL is a DuckDB JDBC URL, false otherwise
     */
    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:duckdb:");
    }

    /**
     * Create a new DuckDB dialect instance.
     *
     * @return new DuckDBDialect instance
     */
    @Override
    public JdbcDialect create() {
        return new DuckDBDialect();
    }

    /**
     * Create a new DuckDB dialect instance with compatibility mode.
     *
     * <p>DuckDB dialect currently ignores compatibility mode and field identifier settings.
     *
     * @param compatibleMode the compatibility mode (not used)
     * @param fieldIde the field identifier style (not used)
     * @return new DuckDBDialect instance
     */
    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        return new DuckDBDialect();
    }
}

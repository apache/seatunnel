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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.duckdb;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions.DECIMAL_TYPE_NARROWING;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions.HANDLE_BLOB_AS_STRING;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions.SCHEMA;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions.URL;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions.USERNAME;

/** Factory for {@link DuckDBCatalog} */
@AutoService(Factory.class)
public class DuckDBCatalogFactory implements CatalogFactory {

    private static final String DEFAULT_SCHEMA_NAME = "main";

    @Override
    public String factoryIdentifier() {
        return DatabaseIdentifier.DUCKDB;
    }

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig config) {
        String url = config.get(JdbcCommonOptions.URL);
        String defaultSchema =
                config.getOptional(JdbcCommonOptions.SCHEMA).orElse(DEFAULT_SCHEMA_NAME);
        JdbcUrlUtil.UrlInfo urlInfo = DuckDBURLParser.parse(url);
        return new DuckDBCatalog(catalogName, urlInfo, defaultSchema);
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URL)
                .optional(USERNAME, PASSWORD, SCHEMA, DECIMAL_TYPE_NARROWING, HANDLE_BLOB_AS_STRING)
                .build();
    }
}

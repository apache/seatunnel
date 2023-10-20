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

package org.apache.seatunnel.connectors.seatunnel.jdbc.utils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class JdbcCatalogUtils {
    private static final String DEFAULT_CATALOG = "default";

    private static ReadonlyConfig extractCatalogConfig(JdbcConnectionConfig config) {
        Map<String, Object> catalogConfig = new HashMap<>();
        catalogConfig.put(JdbcCatalogOptions.BASE_URL.key(), config.getUrl());
        config.getUsername()
                .ifPresent(val -> catalogConfig.put(JdbcCatalogOptions.USERNAME.key(), val));
        config.getPassword()
                .ifPresent(val -> catalogConfig.put(JdbcCatalogOptions.PASSWORD.key(), val));
        return ReadonlyConfig.fromMap(catalogConfig);
    }

    public static Optional<Catalog> findCatalog(JdbcConnectionConfig config, JdbcDialect dialect) {
        ReadonlyConfig catalogConfig = extractCatalogConfig(config);
        return FactoryUtil.createOptionalCatalog(
                dialect.dialectName(),
                catalogConfig,
                JdbcCatalogUtils.class.getClassLoader(),
                dialect.dialectName());
    }
}

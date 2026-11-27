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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.kingbase;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;

/**
 * Catalog factory for KingbaseES.
 *
 * <p>The compatibility mode is taken exclusively from the explicit {@code compatible_mode} option;
 * resolving it opens no connection and never probes the server. An absent mode keeps the native
 * Kingbase catalog, which is the pre-existing behavior. Selecting {@code mysql} does not make the
 * catalog connection-free: it builds a regular {@link MySqlCatalog}, whose constructor opens its
 * usual metadata connection and reads the server version. Only mode selection is probe-free, not
 * the catalog's normal startup work.
 */
@AutoService(Factory.class)
public class KingbaseCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier() {
        return DatabaseIdentifier.KINGBASE;
    }

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        String urlWithDatabase = options.get(JdbcCommonOptions.URL);
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo(urlWithDatabase);
        if (resolvesToMySqlCatalog(options.get(JdbcCommonOptions.COMPATIBLE_MODE))) {
            return createMySqlCatalog(
                    catalogName,
                    options.get(JdbcCommonOptions.USERNAME),
                    options.get(JdbcCommonOptions.PASSWORD),
                    urlInfo,
                    options.get(JdbcCommonOptions.DRIVER));
        }
        return createKingbaseCatalog(
                catalogName,
                options.get(JdbcCommonOptions.USERNAME),
                options.get(JdbcCommonOptions.PASSWORD),
                urlInfo,
                options.get(JdbcCommonOptions.SCHEMA),
                options.get(JdbcCommonOptions.DRIVER));
    }

    @Override
    public OptionRule optionRule() {
        return JdbcCommonOptions.baseCatalogRule()
                .optional(JdbcCommonOptions.COMPATIBLE_MODE)
                .build();
    }

    /**
     * Whether the given explicit compatibility mode selects the MySQL catalog implementation. This
     * is a pure function of configuration: it never contacts the database, so mode selection is
     * deterministic and independent of transient connectivity.
     */
    static boolean resolvesToMySqlCatalog(String compatibleMode) {
        return compatibleMode != null && "mysql".equalsIgnoreCase(compatibleMode.trim());
    }

    MySqlCatalog createMySqlCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String driverClass) {
        return new MySqlCatalog(catalogName, username, pwd, urlInfo, driverClass);
    }

    KingbaseCatalog createKingbaseCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema,
            String driverClass) {
        return new KingbaseCatalog(catalogName, username, pwd, urlInfo, defaultSchema, driverClass);
    }
}

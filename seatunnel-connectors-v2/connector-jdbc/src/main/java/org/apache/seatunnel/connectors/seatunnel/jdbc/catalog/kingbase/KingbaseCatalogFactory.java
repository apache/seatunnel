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

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;

import java.sql.Connection;

@AutoService(Factory.class)
public class KingbaseCatalogFactory implements CatalogFactory {
    private static final Logger LOG = LoggerFactory.getLogger(KingbaseCatalogFactory.class);

    @Override
    public String factoryIdentifier() {
        return DatabaseIdentifier.KINGBASE;
    }

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        String urlWithDatabase = options.get(JdbcCommonOptions.URL);
        Preconditions.checkArgument(
                StringUtils.isNoneBlank(urlWithDatabase),
                "Miss config <base-url>! Please check your config.");
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo(urlWithDatabase);
        String compatibleMode = detectCompatibleMode(options);
        if (isMySQL(compatibleMode)) {
            return getMySqlCatalog(catalogName, options, urlInfo);
        }
        return new KingbaseCatalog(
                catalogName,
                options.get(JdbcCommonOptions.USERNAME),
                options.get(JdbcCommonOptions.PASSWORD),
                urlInfo,
                options.get(JdbcCommonOptions.SCHEMA),
                options.get(JdbcCommonOptions.DRIVER));
    }

    @Override
    public OptionRule optionRule() {
        return JdbcCommonOptions.BASE_CATALOG_RULE.build();
    }

    private String detectCompatibleMode(ReadonlyConfig config) {
        JdbcConnectionConfig jdbcConnectionConfig = JdbcConnectionConfig.of(config);
        SimpleJdbcConnectionProvider provider =
                new SimpleJdbcConnectionProvider(jdbcConnectionConfig);
        try {
            Connection connection = provider.getOrEstablishConnection();
            if (connection instanceof com.kingbase8.jdbc.KbConnection) {
                return ((com.kingbase8.jdbc.KbConnection) connection).getCompatibleLevel();
            }
        } catch (Exception e) {
            LOG.error("Failed to detect compatible mode", e);
        } finally {
            provider.closeConnection();
        }
        return null;
    }

    private MySqlCatalog getMySqlCatalog(
            String catalogName, ReadonlyConfig options, JdbcUrlUtil.UrlInfo urlInfo) {
        return new MySqlCatalog(
                catalogName,
                options.get(JdbcCommonOptions.USERNAME),
                options.get(JdbcCommonOptions.PASSWORD),
                urlInfo,
                options.get(JdbcCommonOptions.DRIVER));
    }

    private boolean isMySQL(String compatibleMode) {
        return "mysql".equalsIgnoreCase(compatibleMode);
    }
}

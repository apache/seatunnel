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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.yashandb;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class YashanDbCatalogFactory implements CatalogFactory {

    private static final String YASHANDB_URL_PREFIX = "jdbc:yasdb://";

    @Override
    public String factoryIdentifier() {
        return DatabaseIdentifier.YASHANDB;
    }

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        String urlWithDatabase = options.get(JdbcCommonOptions.URL);
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo(urlWithDatabase);
        return new YashanDbCatalog(
                catalogName,
                options.get(JdbcCommonOptions.USERNAME),
                options.get(JdbcCommonOptions.PASSWORD),
                urlInfo,
                options.get(JdbcCommonOptions.SCHEMA),
                options.get(JdbcCommonOptions.DRIVER));
    }

    @Override
    public OptionRule optionRule() {
        return JdbcCommonOptions.baseCatalogRule(new YashanDbUrlValidator()).build();
    }

    /**
     * YashanDB-specific URL validator that rejects any JDBC URL not starting with {@code
     * jdbc:yasdb://} before the catalog is created, providing fail-fast behaviour at configuration
     * parsing time.
     */
    static class YashanDbUrlValidator implements ConditionExtension<String> {
        @Override
        public String description() {
            return "YashanDB JDBC URL must start with '"
                    + YASHANDB_URL_PREFIX
                    + "' (e.g. jdbc:yasdb://host:port/database)";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, String url) {
            if (url == null || url.trim().isEmpty()) {
                return false;
            }
            if (!url.startsWith(YASHANDB_URL_PREFIX)) {
                throw new OptionValidationException(
                        String.format(
                                "Invalid YashanDB JDBC URL: [%s], URL must start with '%s'",
                                url, YASHANDB_URL_PREFIX));
            }
            try {
                JdbcUrlUtil.UrlInfo info = JdbcUrlUtil.getUrlInfo(url);
                return info.getHost() != null && !info.getHost().isEmpty();
            } catch (IllegalArgumentException e) {
                throw new OptionValidationException(
                        String.format(
                                "Invalid YashanDB JDBC URL format: [%s], "
                                        + "expected pattern: jdbc:yasdb://host:port[/database][?properties]",
                                url));
            }
        }
    }
}

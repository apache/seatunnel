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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

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
public class OracleCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier() {
        return DatabaseIdentifier.ORACLE;
    }

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        String urlWithDatabase = options.get(JdbcCommonOptions.URL);
        JdbcUrlUtil.UrlInfo urlInfo = OracleURLParser.parse(urlWithDatabase);
        return new OracleCatalog(
                catalogName,
                options.get(JdbcCommonOptions.USERNAME),
                options.get(JdbcCommonOptions.PASSWORD),
                urlInfo,
                options.get(JdbcCommonOptions.SCHEMA),
                options.get(JdbcCommonOptions.DECIMAL_TYPE_NARROWING),
                options.get(JdbcCommonOptions.DRIVER),
                options.getOptional(JdbcCommonOptions.HANDLE_BLOB_AS_STRING).orElse(false));
    }

    @Override
    public OptionRule optionRule() {
        return JdbcCommonOptions.baseCatalogRule(new OracleUrlValidator()).build();
    }

    static class OracleUrlValidator implements ConditionExtension<String> {
        @Override
        public String description() {
            return "Oracle JDBC URL must contain a service name (e.g. jdbc:oracle:thin:@host:port/service)";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, String url) {
            if (url == null || url.trim().isEmpty()) {
                return false;
            }
            try {
                JdbcUrlUtil.UrlInfo info = OracleURLParser.parse(url);
                return StringUtils.isNotBlank(info.getHost())
                        && info.getDefaultDatabase().isPresent();
            } catch (IllegalArgumentException e) {
                throw new OptionValidationException(
                        String.format(
                                "Invalid Oracle JDBC URL format: [%s], "
                                        + "expected pattern: jdbc:oracle:thin:@host:port/service",
                                url));
            }
        }
    }
}

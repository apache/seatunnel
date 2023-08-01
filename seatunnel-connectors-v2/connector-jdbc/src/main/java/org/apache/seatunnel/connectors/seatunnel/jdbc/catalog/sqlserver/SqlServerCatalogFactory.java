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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;

import com.google.auto.service.AutoService;

import java.util.Optional;

@AutoService(Factory.class)
public class SqlServerCatalogFactory implements CatalogFactory {
    public static final String IDENTIFIER = "SqlServer";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        String url = options.get(JdbcCatalogOptions.BASE_URL);
        JdbcUrlUtil.UrlInfo urlInfo = SqlServerURLParser.parse(url);
        Optional<String> defaultDatabase = urlInfo.getDefaultDatabase();
        if (!defaultDatabase.isPresent()) {
            throw new OptionValidationException(JdbcCatalogOptions.BASE_URL);
        }
        return new SqlServerCatalog(
                catalogName,
                options.get(JdbcCatalogOptions.USERNAME),
                options.get(JdbcCatalogOptions.PASSWORD),
                urlInfo,
                options.get(JdbcCatalogOptions.SCHEMA));
    }

    @Override
    public OptionRule optionRule() {
        return JdbcCatalogOptions.BASE_RULE.build();
    }
}

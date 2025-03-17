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

package org.apache.seatunnel.connectors.seatunnel.starrocks.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksSourceOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class StarRocksCatalogFactory implements CatalogFactory {
    public static final String IDENTIFIER = StarRocksSinkOptions.CONNECTOR_IDENTITY;

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        return new StarRocksCatalog(
                catalogName,
                options.get(StarRocksSourceOptions.USERNAME),
                options.get(StarRocksSourceOptions.PASSWORD),
                options.get(StarRocksSinkOptions.BASE_URL),
                options.get(StarRocksSinkOptions.SAVE_MODE_CREATE_TEMPLATE));
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(StarRocksSinkOptions.BASE_URL)
                .required(StarRocksSourceOptions.USERNAME)
                .required(StarRocksSourceOptions.PASSWORD)
                .build();
    }
}

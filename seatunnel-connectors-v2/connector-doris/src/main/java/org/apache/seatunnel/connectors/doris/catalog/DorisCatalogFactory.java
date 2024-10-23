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

package org.apache.seatunnel.connectors.doris.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.connectors.doris.config.DorisOptions;
import org.apache.seatunnel.connectors.doris.config.DorisSinkOptions;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.doris.config.DorisOptions.IDENTIFIER;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.SAVE_MODE_CREATE_TEMPLATE;

@AutoService(Factory.class)
public class DorisCatalogFactory implements CatalogFactory {

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        return new DorisCatalog(
                catalogName,
                options.get(DorisOptions.FENODES),
                options.get(DorisOptions.QUERY_PORT),
                options.get(DorisOptions.USERNAME),
                options.get(DorisOptions.PASSWORD),
                options.get(SAVE_MODE_CREATE_TEMPLATE),
                options.get(DorisSinkOptions.DEFAULT_DATABASE));
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return DorisOptions.CATALOG_RULE.build();
    }
}

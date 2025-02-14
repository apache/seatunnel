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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.ACCESS_ID;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.ACCESS_KEY;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.ENDPOINT;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.PARTITION_SPEC;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.PLUGIN_NAME;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.PROJECT;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.SPLIT_ROW;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.TABLE_NAME;

@AutoService(Factory.class)
public class MaxComputeCatalogFactory implements CatalogFactory {

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        return new MaxComputeCatalog(catalogName, options);
    }

    @Override
    public String factoryIdentifier() {
        return PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(ACCESS_ID, ACCESS_KEY, ENDPOINT, PROJECT, TABLE_NAME)
                .optional(PARTITION_SPEC, SPLIT_ROW, ConnectorCommonOptions.SCHEMA)
                .build();
    }
}

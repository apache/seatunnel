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

package org.apache.seatunnel.connectors.seatunnel.paimon.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonSinkConfig;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class PaimonCatalogFactory implements CatalogFactory {
    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig readonlyConfig) {
        return new PaimonCatalog(catalogName, new PaimonSinkConfig(readonlyConfig));
    }

    @Override
    public String factoryIdentifier() {
        return "Paimon";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        PaimonSinkConfig.WAREHOUSE,
                        PaimonSinkConfig.DATABASE,
                        PaimonSinkConfig.TABLE)
                .optional(
                        PaimonSinkConfig.HDFS_SITE_PATH,
                        PaimonSinkConfig.SCHEMA_SAVE_MODE,
                        PaimonSinkConfig.DATA_SAVE_MODE,
                        PaimonSinkConfig.PRIMARY_KEYS,
                        PaimonSinkConfig.PARTITION_KEYS,
                        PaimonSinkConfig.WRITE_PROPS,
                        PaimonSinkConfig.HADOOP_CONF,
                        PaimonSinkConfig.HADOOP_CONF_PATH)
                .build();
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.iceberg.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_CASE_SENSITIVE;

@AutoService(Factory.class)
public class IcebergCatalogFactory implements CatalogFactory {
    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        return new IcebergCatalog(catalogName, options);
    }

    @Override
    public String factoryIdentifier() {
        return "Iceberg";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        CommonConfig.KEY_CATALOG_NAME,
                        CommonConfig.KEY_NAMESPACE,
                        CommonConfig.KEY_TABLE,
                        CommonConfig.CATALOG_PROPS)
                .optional(
                        CommonConfig.HADOOP_PROPS,
                        CommonConfig.KERBEROS_PRINCIPAL,
                        CommonConfig.KERBEROS_KEYTAB_PATH,
                        CommonConfig.KRB5_PATH,
                        KEY_CASE_SENSITIVE)
                .build();
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.deltalake.catalog;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.connectors.seatunnel.deltalake.config.DeltaLakeCommonOptions;

@AutoService(Factory.class)
public class DeltaLakeCatalogFactory implements CatalogFactory {
    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        return new DeltaLakeCatalog(catalogName, options);
    }

    @Override
    public String factoryIdentifier() {
        return "Iceberg";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        DeltaLakeCommonOptions.KEY_CATALOG_NAME,
                        DeltaLakeCommonOptions.KEY_NAMESPACE,
                        DeltaLakeCommonOptions.KEY_TABLE,
                        DeltaLakeCommonOptions.CATALOG_PROPS)
                .optional(
                        DeltaLakeCommonOptions.HADOOP_PROPS,
                        DeltaLakeCommonOptions.KERBEROS_PRINCIPAL,
                        DeltaLakeCommonOptions.KERBEROS_KEYTAB_PATH,
                        DeltaLakeCommonOptions.KRB5_PATH,
                        DeltaLakeCommonOptions.KEY_CASE_SENSITIVE)
                .build();
    }
}

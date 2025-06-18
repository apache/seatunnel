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

package org.apache.seatunnel.connectors.seatunnel.kudu.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduBaseOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class KuduCatalogFactory implements CatalogFactory {

    public static final String IDENTIFIER = "Kudu";

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        CommonConfig config = new CommonConfig(options);
        KuduCatalog kuduCatalog = new KuduCatalog(catalogName, config);
        return kuduCatalog;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(KuduBaseOptions.MASTER)
                .optional(KuduBaseOptions.WORKER_COUNT)
                .optional(KuduBaseOptions.OPERATION_TIMEOUT)
                .optional(KuduBaseOptions.ADMIN_OPERATION_TIMEOUT)
                .optional(KuduBaseOptions.KERBEROS_KRB5_CONF)
                .optional(KuduBaseOptions.ENABLE_KERBEROS)
                .conditional(
                        KuduBaseOptions.ENABLE_KERBEROS,
                        true,
                        KuduBaseOptions.KERBEROS_PRINCIPAL,
                        KuduBaseOptions.KERBEROS_KEYTAB)
                .build();
    }
}

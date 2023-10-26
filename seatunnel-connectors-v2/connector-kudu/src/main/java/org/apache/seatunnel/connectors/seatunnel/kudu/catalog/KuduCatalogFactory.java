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

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig.ADMIN_OPERATION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig.ENABLE_KERBEROS;
import static org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig.KERBEROS_KEYTAB;
import static org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig.KERBEROS_KRB5_CONF;
import static org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig.KERBEROS_PRINCIPAL;
import static org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig.MASTER;
import static org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig.OPERATION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig.WORKER_COUNT;

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
                .required(MASTER)
                .optional(WORKER_COUNT)
                .optional(OPERATION_TIMEOUT)
                .optional(ADMIN_OPERATION_TIMEOUT)
                .optional(KERBEROS_KRB5_CONF)
                .conditional(ENABLE_KERBEROS, true, KERBEROS_PRINCIPAL, KERBEROS_KEYTAB)
                .build();
    }
}

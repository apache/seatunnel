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

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.HDFS_SITE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.HIVE_SITE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KERBEROS_KEYTAB_PATH;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KERBEROS_KRB5_CONF_PATH;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KERBEROS_PRINCIPAL;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_CATALOG_NAME;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_CATALOG_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_NAMESPACE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_TABLE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_URI;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig.KEY_WAREHOUSE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType.HIVE;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig.ENABLE_UPSERT;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig.PRIMARY_KEYS;

@AutoService(Factory.class)
public class IcebergSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Iceberg";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        KEY_CATALOG_NAME, KEY_CATALOG_TYPE, KEY_WAREHOUSE, KEY_NAMESPACE, KEY_TABLE)
                .conditional(KEY_CATALOG_TYPE, HIVE, KEY_URI)
                .optional(
                        KERBEROS_PRINCIPAL,
                        KERBEROS_KEYTAB_PATH,
                        KERBEROS_KRB5_CONF_PATH,
                        HDFS_SITE_PATH,
                        HIVE_SITE_PATH)
                .optional(ENABLE_UPSERT)
                .optional(PRIMARY_KEYS)
                .build();
    }

    @Override
    public TableSink createSink(TableFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        return () -> new IcebergSink(context.getCatalogTable(), config);
    }
}

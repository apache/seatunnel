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

package org.apache.seatunnel.connectors.seatunnel.kudu.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSinkConfig;

import com.google.auto.service.AutoService;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kudu.client.SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND;
import static org.apache.kudu.client.SessionConfiguration.FlushMode.MANUAL_FLUSH;

@AutoService(Factory.class)
public class KuduSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Kudu";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(KuduSinkConfig.MASTER)
                .optional(KuduSinkConfig.TABLE_NAME)
                .optional(KuduSinkConfig.WORKER_COUNT)
                .optional(KuduSinkConfig.OPERATION_TIMEOUT)
                .optional(KuduSinkConfig.ADMIN_OPERATION_TIMEOUT)
                .optional(KuduSinkConfig.SAVE_MODE)
                .optional(KuduSinkConfig.FLUSH_MODE)
                .optional(KuduSinkConfig.IGNORE_NOT_FOUND)
                .optional(KuduSinkConfig.IGNORE_DUPLICATE)
                .optional(KuduSinkConfig.ENABLE_KERBEROS)
                .optional(KuduSinkConfig.KERBEROS_KRB5_CONF)
                .optional(SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .conditional(
                        KuduSinkConfig.FLUSH_MODE,
                        Arrays.asList(AUTO_FLUSH_BACKGROUND.name(), MANUAL_FLUSH.name()),
                        KuduSinkConfig.BATCH_SIZE)
                .conditional(
                        KuduSinkConfig.FLUSH_MODE,
                        AUTO_FLUSH_BACKGROUND.name(),
                        KuduSinkConfig.BUFFER_FLUSH_INTERVAL)
                .conditional(
                        KuduSinkConfig.ENABLE_KERBEROS,
                        true,
                        KuduSinkConfig.KERBEROS_PRINCIPAL,
                        KuduSinkConfig.KERBEROS_KEYTAB)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        if (!config.getOptional(KuduSinkConfig.TABLE_NAME).isPresent()) {
            Map<String, String> map = config.toMap();
            map.put(
                    KuduSinkConfig.TABLE_NAME.key(),
                    catalogTable.getTableId().toTablePath().getFullName());
            config = ReadonlyConfig.fromMap(new HashMap<>(map));
        }
        KuduSinkConfig kuduSinkConfig = new KuduSinkConfig(config);
        return () -> new KuduSink(kuduSinkConfig, catalogTable);
    }
}

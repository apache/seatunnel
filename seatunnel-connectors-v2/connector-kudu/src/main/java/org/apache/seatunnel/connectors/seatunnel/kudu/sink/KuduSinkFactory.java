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
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSinkOptions;

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
                .required(KuduSinkOptions.MASTER)
                .optional(KuduSinkOptions.TABLE_NAME)
                .optional(KuduSinkOptions.WORKER_COUNT)
                .optional(KuduSinkOptions.OPERATION_TIMEOUT)
                .optional(KuduSinkOptions.ADMIN_OPERATION_TIMEOUT)
                .optional(KuduSinkOptions.SAVE_MODE)
                .optional(KuduSinkOptions.FLUSH_MODE)
                .optional(KuduSinkOptions.IGNORE_NOT_FOUND)
                .optional(KuduSinkOptions.IGNORE_DUPLICATE)
                .optional(KuduSinkOptions.ENABLE_KERBEROS)
                .optional(KuduSinkOptions.KERBEROS_KRB5_CONF)
                .optional(SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .conditional(
                        KuduSinkOptions.FLUSH_MODE,
                        Arrays.asList(AUTO_FLUSH_BACKGROUND.name(), MANUAL_FLUSH.name()),
                        KuduSinkOptions.BATCH_SIZE)
                .conditional(
                        KuduSinkOptions.FLUSH_MODE,
                        AUTO_FLUSH_BACKGROUND.name(),
                        KuduSinkOptions.BUFFER_FLUSH_INTERVAL)
                .conditional(
                        KuduSinkOptions.ENABLE_KERBEROS,
                        true,
                        KuduSinkOptions.KERBEROS_PRINCIPAL,
                        KuduSinkOptions.KERBEROS_KEYTAB)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        if (!config.getOptional(KuduSinkOptions.TABLE_NAME).isPresent()) {
            Map<String, String> map = config.toMap();
            map.put(
                    KuduSinkOptions.TABLE_NAME.key(),
                    catalogTable.getTableId().toTablePath().getFullName());
            config = ReadonlyConfig.fromMap(new HashMap<>(map));
        }
        KuduSinkConfig kuduSinkConfig = new KuduSinkConfig(config);
        return () -> new KuduSink(kuduSinkConfig, catalogTable);
    }
}

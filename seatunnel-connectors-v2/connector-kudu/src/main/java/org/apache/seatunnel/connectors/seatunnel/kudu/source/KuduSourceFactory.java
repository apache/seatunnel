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

package org.apache.seatunnel.connectors.seatunnel.kudu.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSourceOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;

@AutoService(Factory.class)
public class KuduSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "Kudu";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(KuduSourceOptions.MASTER)
                .optional(KuduSourceOptions.SCHEMA)
                .optional(KuduSourceOptions.WORKER_COUNT)
                .optional(KuduSourceOptions.OPERATION_TIMEOUT)
                .optional(KuduSourceOptions.ADMIN_OPERATION_TIMEOUT)
                .optional(KuduSourceOptions.QUERY_TIMEOUT)
                .optional(KuduSourceOptions.SCAN_BATCH_SIZE_BYTES)
                .optional(KuduSourceOptions.FILTER)
                .optional(KuduSourceOptions.ENABLE_KERBEROS)
                .optional(KuduSourceOptions.KERBEROS_KRB5_CONF)
                .exclusive(KuduSourceOptions.TABLE_NAME, ConnectorCommonOptions.TABLE_LIST)
                .conditional(
                        KuduSourceOptions.ENABLE_KERBEROS,
                        true,
                        KuduSourceOptions.KERBEROS_PRINCIPAL,
                        KuduSourceOptions.KERBEROS_KEYTAB)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return KuduSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        KuduSourceConfig kuduSourceConfig = new KuduSourceConfig(config);
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new KuduSource(kuduSourceConfig);
    }
}

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
package org.apache.seatunnel.connectors.seatunnel.prometheus.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.http.sink.HttpSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSinkConfig;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class PrometheusSinkFactory extends HttpSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Prometheus";
    }

    public TableSink createSink(TableSinkFactoryContext context) {

        ReadonlyConfig readonlyConfig = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        return () -> new PrometheusSink(readonlyConfig, catalogTable);
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(PrometheusSinkConfig.URL)
                .required(PrometheusSinkConfig.KEY_LABEL)
                .required(PrometheusSinkConfig.KEY_VALUE)
                .optional(PrometheusSinkConfig.KEY_TIMESTAMP)
                .optional(PrometheusSinkConfig.HEADERS)
                .optional(PrometheusSinkConfig.RETRY)
                .optional(PrometheusSinkConfig.RETRY_BACKOFF_MULTIPLIER_MS)
                .optional(PrometheusSinkConfig.RETRY_BACKOFF_MAX_MS)
                .optional(PrometheusSinkConfig.BATCH_SIZE)
                .optional(PrometheusSinkConfig.FLUSH_INTERVAL)
                .optional(SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .build();
    }
}

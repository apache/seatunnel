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

package org.apache.seatunnel.connectors.seatunnel.deeplake.sink;

import org.apache.seatunnel.api.configuration.util.Condition;
import org.apache.seatunnel.api.configuration.util.ConditionOperator;
import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.deeplake.config.DeepLakeSinkOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class DeepLakeSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return DeepLakeSinkOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () -> new DeepLakeSink(context.getOptions(), context.getCatalogTable());
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        DeepLakeSinkOptions.API_KEY,
                        Conditions.notBlank(DeepLakeSinkOptions.API_KEY))
                .required(
                        DeepLakeSinkOptions.ORG_ID, Conditions.notBlank(DeepLakeSinkOptions.ORG_ID))
                .required(
                        DeepLakeSinkOptions.WORKSPACE,
                        Conditions.notBlank(DeepLakeSinkOptions.WORKSPACE))
                .optional(
                        DeepLakeSinkOptions.API_URL,
                        Conditions.notBlank(DeepLakeSinkOptions.API_URL))
                .optional(DeepLakeSinkOptions.TABLE, Conditions.notBlank(DeepLakeSinkOptions.TABLE))
                .optional(
                        DeepLakeSinkOptions.BATCH_SIZE,
                        Conditions.greaterThan(DeepLakeSinkOptions.BATCH_SIZE, 0))
                .optional(
                        DeepLakeSinkOptions.CONNECT_TIMEOUT_MS,
                        Conditions.greaterThan(DeepLakeSinkOptions.CONNECT_TIMEOUT_MS, 0))
                .optional(
                        DeepLakeSinkOptions.SOCKET_TIMEOUT_MS,
                        Conditions.greaterThan(DeepLakeSinkOptions.SOCKET_TIMEOUT_MS, 0))
                .optional(
                        DeepLakeSinkOptions.SCHEMA_SAVE_MODE,
                        Condition.of(
                                DeepLakeSinkOptions.SCHEMA_SAVE_MODE,
                                ConditionOperator.NOT_EQUAL,
                                SchemaSaveMode.RECREATE_SCHEMA))
                .optional(SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .build();
    }
}

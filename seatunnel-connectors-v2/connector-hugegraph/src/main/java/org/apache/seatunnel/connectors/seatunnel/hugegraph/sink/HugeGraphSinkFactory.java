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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphOptions;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSinkOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class HugeGraphSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return HugeGraphOptions.PLUGIN_NAME;
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        HugeGraphSinkConfig sinkConfig = HugeGraphSinkConfig.of(context.getOptions());
        return () -> new HugeGraphSink(sinkConfig, context.getCatalogTable());
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                // connection config
                .required(HugeGraphOptions.HOST, HugeGraphOptions.PORT, HugeGraphOptions.GRAPH_NAME)
                .optional(
                        HugeGraphOptions.PROTOCOL,
                        HugeGraphOptions.USERNAME,
                        HugeGraphOptions.PASSWORD,
                        // Optional connection setting passed through to select the HugeGraph graph
                        // space (defaults to "DEFAULT").
                        HugeGraphOptions.GRAPH_SPACE)
                // mapping config: mappings (new) or schema_config (legacy)
                .optional(HugeGraphSinkOptions.MAPPINGS, HugeGraphSinkOptions.SCHEMA_CONFIG)
                // schema and data save mode
                .optional(
                        HugeGraphSinkOptions.SCHEMA_SAVE_MODE,
                        HugeGraphSinkOptions.DATA_SAVE_MODE,
                        HugeGraphSinkOptions.DELETE_VERTEX_WITH_EDGES,
                        HugeGraphSinkOptions.ALLOW_CASCADE_DELETE_UNMAPPED_EDGES)
                // batch config
                .optional(
                        HugeGraphOptions.BATCH_SIZE,
                        HugeGraphOptions.BATCH_INTERVAL_MS,
                        HugeGraphOptions.CHECK_VERTEX)
                // required by the multi-table sink SPI (HugeGraphSink implements
                // SupportMultiTableSink): lets the framework size per-table write replicas
                .optional(SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                // error handling
                .optional(
                        HugeGraphOptions.BATCH_FAILURE_FALLBACK,
                        HugeGraphOptions.MAX_INSERT_ERRORS,
                        HugeGraphOptions.FAILURE_DATA_PATH)
                // retry config
                .optional(
                        HugeGraphOptions.MAX_RETRIES,
                        HugeGraphOptions.RETRY_BACKOFF_MS,
                        HugeGraphOptions.RETRY_BACKOFF_MAX_MS)
                // deprecated field selection
                .optional(HugeGraphSinkOptions.SELECTED_FIELDS, HugeGraphSinkOptions.IGNORED_FIELDS)
                .build();
    }
}

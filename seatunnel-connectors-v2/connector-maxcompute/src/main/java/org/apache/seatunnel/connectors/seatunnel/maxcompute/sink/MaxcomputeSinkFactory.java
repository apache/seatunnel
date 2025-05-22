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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.options.table.FormatOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeSinkOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class MaxcomputeSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return MaxcomputeSinkOptions.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        MaxcomputeSinkOptions.ACCESS_ID,
                        MaxcomputeSinkOptions.ACCESS_KEY,
                        MaxcomputeSinkOptions.ENDPOINT,
                        MaxcomputeSinkOptions.PROJECT,
                        MaxcomputeSinkOptions.TABLE_NAME)
                .optional(
                        MaxcomputeSinkOptions.PARTITION_SPEC,
                        MaxcomputeSinkOptions.OVERWRITE,
                        MaxcomputeSinkOptions.SCHEMA_SAVE_MODE,
                        MaxcomputeSinkOptions.DATA_SAVE_MODE,
                        MaxcomputeSinkOptions.SAVE_MODE_CREATE_TEMPLATE,
                        MaxcomputeSinkOptions.CUSTOM_SQL,
                        FormatOptions.DATETIME_FORMAT,
                        SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () ->
                new MaxcomputeSink(
                        context.getOptions(),
                        CatalogTable.of(
                                TableIdentifier.of(
                                        context.getCatalogTable().getCatalogName(),
                                        context.getOptions().get(MaxcomputeSinkOptions.PROJECT),
                                        context.getOptions().get(MaxcomputeSinkOptions.TABLE_NAME)),
                                context.getCatalogTable()));
    }
}

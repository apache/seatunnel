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

package org.apache.seatunnel.core.starter.flink.execution;

import static org.apache.seatunnel.apis.base.plugin.Plugin.RESULT_TABLE_NAME;
import static org.apache.seatunnel.apis.base.plugin.Plugin.SOURCE_TABLE_NAME;

import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.util.TableUtil;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Optional;

public abstract class AbstractPluginExecuteProcessor<T> implements PluginExecuteProcessor {

    protected final FlinkEnvironment flinkEnvironment;
    protected final List<? extends Config> pluginConfigs;
    protected final List<T> plugins;

    protected AbstractPluginExecuteProcessor(FlinkEnvironment flinkEnvironment,
                                             List<? extends Config> pluginConfigs) {
        this.flinkEnvironment = flinkEnvironment;
        this.pluginConfigs = pluginConfigs;
        this.plugins = initializePlugins(pluginConfigs);
    }

    protected abstract List<T> initializePlugins(List<? extends Config> pluginConfigs);

    protected void registerResultTable(Config pluginConfig, DataStream<Row> dataStream) {
        if (pluginConfig.hasPath(RESULT_TABLE_NAME)) {
            String name = pluginConfig.getString(RESULT_TABLE_NAME);
            StreamTableEnvironment tableEnvironment = flinkEnvironment.getStreamTableEnvironment();
            if (!TableUtil.tableExists(tableEnvironment, name)) {
                if (pluginConfig.hasPath("field_name")) {
                    String fieldName = pluginConfig.getString("field_name");
                    tableEnvironment.registerDataStream(name, dataStream, fieldName);
                } else {
                    tableEnvironment.registerDataStream(name, dataStream);
                }
            }
        }
    }

    protected Optional<DataStream<Row>> fromSourceTable(Config pluginConfig) {
        if (pluginConfig.hasPath(SOURCE_TABLE_NAME)) {
            StreamTableEnvironment tableEnvironment = flinkEnvironment.getStreamTableEnvironment();
            Table table = tableEnvironment.scan(pluginConfig.getString(SOURCE_TABLE_NAME));
            return Optional.ofNullable(TableUtil.tableToDataStream(tableEnvironment, table, true));
        }
        return Optional.empty();
    }
}

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

package org.apache.seatunnel.flink.stream;

import org.apache.seatunnel.apis.base.env.Execution;
import org.apache.seatunnel.apis.base.plugin.Plugin;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.util.TableUtil;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
public class FlinkStreamExecution implements Execution<FlinkStreamSource, FlinkStreamTransform, FlinkStreamSink, FlinkEnvironment> {

    private Config config;

    private final FlinkEnvironment flinkEnvironment;

    public FlinkStreamExecution(FlinkEnvironment streamEnvironment) {
        this.flinkEnvironment = streamEnvironment;
    }

    @Override
    public void start(List<FlinkStreamSource> sources, List<FlinkStreamTransform> transforms, List<FlinkStreamSink> sinks) throws Exception {
        List<DataStream<Row>> data = new ArrayList<>();

        for (FlinkStreamSource source : sources) {
            DataStream<Row> dataStream = source.getData(flinkEnvironment);
            data.add(dataStream);
            registerResultTable(source, dataStream);
        }

        DataStream<Row> input = data.get(0);

        for (FlinkStreamTransform transform : transforms) {
            DataStream<Row> stream = fromSourceTable(transform.getConfig()).orElse(input);
            input = transform.processStream(flinkEnvironment, stream);
            registerResultTable(transform, input);
            transform.registerFunction(flinkEnvironment);
        }

        for (FlinkStreamSink sink : sinks) {
            DataStream<Row> stream = fromSourceTable(sink.getConfig()).orElse(input);
            sink.outputStream(flinkEnvironment, stream);
        }
        try {
            log.info("Flink Execution Plan:{}", flinkEnvironment.getStreamExecutionEnvironment().getExecutionPlan());
            flinkEnvironment.getStreamExecutionEnvironment().execute(flinkEnvironment.getJobName());
        } catch (Exception e) {
            log.warn("Flink with job name [{}] execute failed", flinkEnvironment.getJobName());
            throw e;
        }
    }

    private void registerResultTable(Plugin<FlinkEnvironment> plugin, DataStream<Row> dataStream) {
        Config config = plugin.getConfig();
        flinkEnvironment.registerResultTable(config, dataStream);
    }

    private Optional<DataStream<Row>> fromSourceTable(Config pluginConfig) {
        if (pluginConfig.hasPath(SOURCE_TABLE_NAME)) {
            StreamTableEnvironment tableEnvironment = flinkEnvironment.getStreamTableEnvironment();
            Table table = tableEnvironment.scan(pluginConfig.getString(SOURCE_TABLE_NAME));
            return Optional.ofNullable(TableUtil.tableToDataStream(tableEnvironment, table, true));
        }
        return Optional.empty();
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

}

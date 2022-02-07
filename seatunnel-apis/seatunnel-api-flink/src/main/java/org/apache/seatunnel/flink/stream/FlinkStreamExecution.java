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

import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.env.Execution;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.util.TableUtil;
import org.apache.seatunnel.plugin.Plugin;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FlinkStreamExecution implements Execution<FlinkStreamSource, FlinkStreamTransform, FlinkStreamSink> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkStreamExecution.class);

    private Config config;

    private FlinkEnvironment flinkEnvironment;

    public FlinkStreamExecution(FlinkEnvironment streamEnvironment) {
        this.flinkEnvironment = streamEnvironment;
    }

    @Override
    public void start(List<FlinkStreamSource> sources, List<FlinkStreamTransform> transforms, List<FlinkStreamSink> sinks) throws Exception {
        List<DataStream> data = new ArrayList<>();

        for (FlinkStreamSource source : sources) {
            DataStream dataStream = source.getData(flinkEnvironment);
            data.add(dataStream);
            registerResultTable(source, dataStream);
        }

        DataStream input = data.get(0);

        for (FlinkStreamTransform transform : transforms) {
            DataStream stream = fromSourceTable(transform);
            if (Objects.isNull(stream)) {
                stream = input;
            }
            input = transform.processStream(flinkEnvironment, stream);
            registerResultTable(transform, input);
            transform.registerFunction(flinkEnvironment);
        }

        for (FlinkStreamSink sink : sinks) {
            DataStream stream = fromSourceTable(sink);
            if (Objects.isNull(stream)) {
                stream = input;
            }
            sink.outputStream(flinkEnvironment, stream);
        }
        try {
            LOGGER.info("Flink Execution Plan:{}", flinkEnvironment.getStreamExecutionEnvironment().getExecutionPlan());
            flinkEnvironment.getStreamExecutionEnvironment().execute(flinkEnvironment.getJobName());
        } catch (Exception e) {
            LOGGER.warn("Flink with job name [{}] execute failed", flinkEnvironment.getJobName());
            throw e;
        }
    }

    private void registerResultTable(Plugin plugin, DataStream dataStream) {
        Config config = plugin.getConfig();
        if (config.hasPath(RESULT_TABLE_NAME)) {
            String name = config.getString(RESULT_TABLE_NAME);
            StreamTableEnvironment tableEnvironment = flinkEnvironment.getStreamTableEnvironment();
            if (!TableUtil.tableExists(tableEnvironment, name)) {
                if (config.hasPath("field_name")) {
                    String fieldName = config.getString("field_name");
                    tableEnvironment.registerDataStream(name, dataStream, fieldName);
                } else {
                    tableEnvironment.registerDataStream(name, dataStream);
                }
            }
        }
    }

    private DataStream fromSourceTable(Plugin plugin) {
        Config config = plugin.getConfig();
        if (config.hasPath(SOURCE_TABLE_NAME)) {
            StreamTableEnvironment tableEnvironment = flinkEnvironment.getStreamTableEnvironment();
            Table table = tableEnvironment.scan(config.getString(SOURCE_TABLE_NAME));
            return TableUtil.tableToDataStream(tableEnvironment, table, true);
        }
        return null;
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return CheckResult.success();
    }

    @Override
    public void prepare(Void prepareEnv) {
    }
}

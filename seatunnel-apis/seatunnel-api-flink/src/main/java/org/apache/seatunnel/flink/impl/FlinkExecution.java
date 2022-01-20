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

package org.apache.seatunnel.flink.impl;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.env.Execution;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.flink.util.TableUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.plugin.Plugin;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FlinkExecution implements Execution<FlinkSource, FlinkTransform, FlinkSink> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkExecution.class);

    private Config config;

    private FlinkEnvironment flinkEnvironment;

    public FlinkExecution(FlinkEnvironment streamEnvironment) {
        this.flinkEnvironment = streamEnvironment;
    }

    @Override
    public void start(List<FlinkSource> sources, List<FlinkTransform> transforms, List<FlinkSink> sinks) {
        List<DataStream<RowData>> data = new ArrayList<>();

        for (FlinkSource source : sources) {
            DataStream<RowData> dataStream = source.getData(flinkEnvironment);
            data.add(dataStream);
            registerResultTable(source, dataStream);
        }

        DataStream<RowData> input = data.get(0);

        for (FlinkTransform transform : transforms) {
            DataStream<RowData> stream = fromSourceTable(transform);
            if (Objects.isNull(stream)) {
                stream = input;
            }
            input = transform.process(flinkEnvironment, stream);
            registerResultTable(transform, input);
            transform.registerFunction(flinkEnvironment);
        }

        for (FlinkSink sink : sinks) {
            DataStream<RowData> stream = fromSourceTable(sink);
            if (Objects.isNull(stream)) {
                stream = input;
            }
            sink.output(flinkEnvironment,stream);
        }
        try {
            LOGGER.info("Flink Execution Plan:{}", flinkEnvironment.getStreamExecutionEnvironment().getExecutionPlan());
            flinkEnvironment.getStreamExecutionEnvironment().execute(flinkEnvironment.getJobName());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void registerResultTable(Plugin plugin, DataStream<RowData> dataStream) {
        Config config = plugin.getConfig();
        if (config.hasPath(RESULT_TABLE_NAME)) {
            String name = config.getString(RESULT_TABLE_NAME);
            StreamTableEnvironment tableEnvironment = flinkEnvironment.getStreamTableEnvironment();
            if (!TableUtil.tableExists(tableEnvironment, name)) {
                if (config.hasPath("field_name")) {
                    String fieldName = config.getString("field_name");
                    tableEnvironment.createTemporaryView(name, dataStream,fieldName);
                } else {
                    tableEnvironment.createTemporaryView(name, dataStream);
                }
            }
        }
    }

    private DataStream<RowData> fromSourceTable(Plugin plugin) {
        Config config = plugin.getConfig();
        if (config.hasPath(SOURCE_TABLE_NAME)) {
            StreamTableEnvironment tableEnvironment = flinkEnvironment.getStreamTableEnvironment();
            Table table = tableEnvironment.from(config.getString(SOURCE_TABLE_NAME));
            // default use retractstream
            return TableUtil.tableToDataStream(tableEnvironment, table, false);
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
        return new CheckResult(true, Constants.CHECK_SUCCESS);
    }

    @Override
    public void prepare(Void prepareEnv) {
    }
}

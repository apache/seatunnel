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

package org.apache.seatunnel.flink.batch;

import org.apache.seatunnel.apis.base.env.Execution;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.util.TableUtil;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
public class FlinkBatchExecution implements Execution<FlinkBatchSource, FlinkBatchTransform, FlinkBatchSink, FlinkEnvironment> {

    private Config config;

    private final FlinkEnvironment flinkEnvironment;

    public FlinkBatchExecution(FlinkEnvironment flinkEnvironment) {
        this.flinkEnvironment = flinkEnvironment;
    }

    @Override
    public void start(List<FlinkBatchSource> sources, List<FlinkBatchTransform> transforms, List<FlinkBatchSink> sinks) throws Exception {
        List<DataSet<Row>> data = new ArrayList<>();

        for (FlinkBatchSource source : sources) {
            DataSet<Row> dataSet = source.getData(flinkEnvironment);
            data.add(dataSet);
            registerResultTable(source.getConfig(), dataSet);
        }

        DataSet<Row> input = data.get(0);

        for (FlinkBatchTransform transform : transforms) {
            DataSet<Row> dataSet = fromSourceTable(transform.getConfig()).orElse(input);
            input = transform.processBatch(flinkEnvironment, dataSet);
            registerResultTable(transform.getConfig(), input);
            transform.registerFunction(flinkEnvironment);
        }

        for (FlinkBatchSink sink : sinks) {
            DataSet<Row> dataSet = fromSourceTable(sink.getConfig()).orElse(input);
            sink.outputBatch(flinkEnvironment, dataSet);
        }

        if (whetherExecute(sinks)) {
            try {
                log.info("Flink Execution Plan:{}", flinkEnvironment.getBatchEnvironment().getExecutionPlan());
                JobExecutionResult execute = flinkEnvironment.getBatchEnvironment().execute(flinkEnvironment.getJobName());
                log.info(execute.toString());
            } catch (Exception e) {
                log.warn("Flink with job name [{}] execute failed", flinkEnvironment.getJobName());
                throw e;
            }
        }
    }

    private void registerResultTable(Config pluginConfig, DataSet<Row> dataSet) {
        if (pluginConfig.hasPath(RESULT_TABLE_NAME)) {
            String name = pluginConfig.getString(RESULT_TABLE_NAME);
            BatchTableEnvironment tableEnvironment = flinkEnvironment.getBatchTableEnvironment();
            if (!TableUtil.tableExists(tableEnvironment, name)) {
                if (pluginConfig.hasPath("field_name")) {
                    String fieldName = pluginConfig.getString("field_name");
                    tableEnvironment.registerDataSet(name, dataSet, fieldName);
                } else {
                    tableEnvironment.registerDataSet(name, dataSet);
                }
            }
        }
    }

    private Optional<DataSet<Row>> fromSourceTable(Config pluginConfig) {
        if (pluginConfig.hasPath(SOURCE_TABLE_NAME)) {
            BatchTableEnvironment tableEnvironment = flinkEnvironment.getBatchTableEnvironment();
            Table table = tableEnvironment.scan(pluginConfig.getString(SOURCE_TABLE_NAME));
            return Optional.ofNullable(TableUtil.tableToDataSet(tableEnvironment, table));
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

    private boolean whetherExecute(List<FlinkBatchSink> sinks) {
        return sinks.stream().noneMatch(s -> "ConsoleSink".equals(s.getPluginName()) || "AssertSink".equals(s.getPluginName()));
    }
}

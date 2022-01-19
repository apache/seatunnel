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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.env.Execution;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.util.TableUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.plugin.Plugin;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FlinkBatchExecution implements Execution<FlinkBatchSource, FlinkBatchTransform, FlinkBatchSink> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkBatchExecution.class);

    private Config config;

    private FlinkEnvironment flinkEnvironment;

    public FlinkBatchExecution(FlinkEnvironment flinkEnvironment) {
        this.flinkEnvironment = flinkEnvironment;
    }

    @Override
    public void start(List<FlinkBatchSource> sources, List<FlinkBatchTransform> transforms, List<FlinkBatchSink> sinks) {
        List<DataSet> data = new ArrayList<>();

        for (FlinkBatchSource source : sources) {
            DataSet dataSet = source.getData(flinkEnvironment);
            data.add(dataSet);
            registerResultTable(source, dataSet);
        }

        DataSet input = data.get(0);

        for (FlinkBatchTransform transform : transforms) {
            DataSet dataSet = fromSourceTable(transform);
            if (Objects.isNull(dataSet)) {
                dataSet = input;
            }
            input = transform.processBatch(flinkEnvironment, dataSet);
            registerResultTable(transform, input);
        }

        for (FlinkBatchSink sink : sinks) {
            DataSet dataSet = fromSourceTable(sink);
            if (Objects.isNull(dataSet)) {
                dataSet = input;
            }
            sink.outputBatch(flinkEnvironment, dataSet);
        }

        try {
            LOGGER.info("Flink Execution Plan:{}", flinkEnvironment.getBatchEnvironment().getExecutionPlan());
            flinkEnvironment.getBatchEnvironment().execute(flinkEnvironment.getJobName());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void registerResultTable(Plugin plugin, DataSet dataSet) {
        Config config = plugin.getConfig();
        if (config.hasPath(RESULT_TABLE_NAME)) {
            String name = config.getString(RESULT_TABLE_NAME);
            BatchTableEnvironment tableEnvironment = flinkEnvironment.getBatchTableEnvironment();
            if (!TableUtil.tableExists(tableEnvironment, name)) {
                if (config.hasPath("field_name")) {
                    String fieldName = config.getString("field_name");
                    tableEnvironment.registerDataSet(name, dataSet, fieldName);
                } else {
                    tableEnvironment.registerDataSet(name, dataSet);
                }
            }
        }
    }

    private DataSet fromSourceTable(Plugin plugin) {
        Config config = plugin.getConfig();
        if (config.hasPath(SOURCE_TABLE_NAME)) {
            BatchTableEnvironment tableEnvironment = flinkEnvironment.getBatchTableEnvironment();
            Table table = tableEnvironment.scan(config.getString(SOURCE_TABLE_NAME));
            return TableUtil.tableToDataSet(tableEnvironment, table);
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

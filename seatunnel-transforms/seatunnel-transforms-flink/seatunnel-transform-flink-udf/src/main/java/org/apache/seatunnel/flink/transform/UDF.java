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

package org.apache.seatunnel.flink.transform;

import org.apache.seatunnel.common.PropertiesUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchTransform;
import org.apache.seatunnel.flink.stream.FlinkStreamTransform;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@SuppressWarnings("PMD")
@Slf4j
public class UDF implements FlinkStreamTransform, FlinkBatchTransform {

    private static final String UDF_CONFIG_PREFIX = "function.";

    private Config config;
    private List<String> classNames;
    private List<String> functionNames;

    @Override
    public DataSet<Row> processBatch(FlinkEnvironment env, DataSet<Row> data) {
        return data;
    }

    @Override
    public DataStream<Row> processStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        return dataStream;
    }

    @Override
    public void registerFunction(FlinkEnvironment flinkEnvironment) {
        TableEnvironment tEnv = flinkEnvironment.isStreaming() ?
                flinkEnvironment.getStreamTableEnvironment() : flinkEnvironment.getBatchTableEnvironment();

        for (int i = 0; i < functionNames.size(); i++) {
            try {
                tEnv.createTemporarySystemFunction(functionNames.get(i), (Class<? extends UserDefinedFunction>) Class.forName(classNames.get(i)));
            } catch (ClassNotFoundException e) {
                log.error("The udf class {} not founded, make sure you enter the correct class name", classNames.get(i));
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public CheckResult checkConfig() {
        hasSubConfig(UDF_CONFIG_PREFIX);
        return CheckResult.success();
    }

    @Override
    public void prepare(FlinkEnvironment prepareEnv) {
        final Properties properties = new Properties();
        PropertiesUtil.setProperties(config, properties, UDF_CONFIG_PREFIX, false);

        classNames = new ArrayList<>(properties.size());
        functionNames = new ArrayList<>(properties.size());

        properties.forEach((k, v) -> {
            classNames.add(String.valueOf(v));
            functionNames.add(String.valueOf(k));
        });
    }

    @Override
    public String getPluginName() {
        return "udf";
    }

    private void hasSubConfig(String prefix) {
        for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                return;
            }
        }
        throw new RuntimeException(String.format("No config start with %s!, please check your transform config!", prefix));
    }
}

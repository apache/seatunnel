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

import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchTransform;
import org.apache.seatunnel.flink.stream.FlinkStreamTransform;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Udf implements FlinkStreamTransform, FlinkBatchTransform {

    private static final Logger LOGGER = LoggerFactory.getLogger(Udf.class);

    private static final String CLASS_NAMES = "class_names";
    private static final String FUNCTION_NAMES = "function_names";
    private Config config;
    private List<String> className;
    private List<String> functionName;

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

        for (int i = 0; i < functionName.size(); i++) {
            try {
                tEnv.createTemporarySystemFunction(functionName.get(i), (Class<? extends UserDefinedFunction>) Class.forName(className.get(i)));
            } catch (ClassNotFoundException e) {
                LOGGER.error("The udf class {} not founded, make sure you enter the correct class name", className.get(i));
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
        return CheckConfigUtil.checkAllExists(config, CLASS_NAMES, FUNCTION_NAMES);
    }

    @Override
    public void prepare(FlinkEnvironment prepareEnv) {
        className = config.getStringList(CLASS_NAMES);
        functionName = config.getStringList(FUNCTION_NAMES);

        Preconditions.checkState(className.size() == functionName.size(),
                "The number of Class names is inconsistent with the number of Function names");
    }

    @Override
    public String getPluginName() {
        return "udf";
    }
}

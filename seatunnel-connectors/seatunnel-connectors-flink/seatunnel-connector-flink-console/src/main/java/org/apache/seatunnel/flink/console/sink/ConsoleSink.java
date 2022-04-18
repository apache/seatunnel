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

package org.apache.seatunnel.flink.console.sink;

import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSink;
import org.apache.seatunnel.flink.stream.FlinkStreamSink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsoleSink extends RichOutputFormat<Row> implements FlinkBatchSink, FlinkStreamSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsoleSink.class);
    private static final String LIMIT = "limit";
    private Integer limit = Integer.MAX_VALUE;

    private static final long serialVersionUID = 3482649370594181723L;
    private Config config;

    @Override
    public void outputBatch(FlinkEnvironment env, DataSet<Row> rowDataSet) {
        try {
            rowDataSet.first(limit).print();
        } catch (Exception e) {
            LOGGER.error("Failed to print result! ", e);
        }
    }

    @Override
    public void outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        dataStream.print();
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
        if (config.hasPath(LIMIT) && config.getInt(LIMIT) >= -1) {
            limit = config.getInt(LIMIT);
        }
        return CheckResult.success();
    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) {

    }

    @Override
    public void writeRecord(Row record) {
    }

    @Override
    public void close() {

    }

    @Override
    public String getPluginName() {
        return "ConsoleSink";
    }

}

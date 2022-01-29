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

package org.apache.seatunnel.flink.sink;

import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.types.Row;

public class DruidSink implements FlinkBatchSink<Row, Row> {

    private static final long serialVersionUID = -2967782261362988646L;
    private static final String COORDINATOR_URL = "coordinator_url";
    private static final String DATASOURCE = "datasource";
    private static final String TIMESTAMP_COLUMN = "timestamp_column";
    private static final String TIMESTAMP_FORMAT = "timestamp_format";

    private Config config;
    private String coordinatorURL;
    private String datasource;
    private String timestampColumn;
    private String timestampFormat;

    @Override
    public DataSink<Row> outputBatch(FlinkEnvironment env, DataSet<Row> dataSet) {
        return dataSet.output(new DruidOutputFormat(coordinatorURL, datasource, timestampColumn, timestampFormat));
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
        return CheckConfigUtil.checkAllExists(config, COORDINATOR_URL, DATASOURCE);
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        this.coordinatorURL = config.getString(COORDINATOR_URL);
        this.datasource = config.getString(DATASOURCE);
        this.timestampColumn = config.hasPath(TIMESTAMP_COLUMN) ? config.getString(TIMESTAMP_COLUMN) : null;
        this.timestampFormat = config.hasPath(TIMESTAMP_FORMAT) ? config.getString(TIMESTAMP_FORMAT) : null;
    }
}

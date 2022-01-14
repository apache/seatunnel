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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.types.Row;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSink;

import java.util.List;

public class InfluxDbSink implements FlinkBatchSink<Row, Row> {

    private static final String SERVER_URL = "server_url";
    private static final String DATABASE = "database";
    private static final String MEASUREMENT = "measurement";
    private static final String TAGS = "tags";
    private static final String FIELDS = "fields";

    private Config config;
    private String serverURL;
    private String database;
    private String measurement;
    private List<String> tags;
    private List<String> fields;

    @Override
    public DataSink<Row> outputBatch(FlinkEnvironment env, DataSet<Row> dataSet) {
        return dataSet.output(new InfluxDbOutputFormat(serverURL, database, measurement, tags, fields));
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
        return CheckConfigUtil.check(config, SERVER_URL, DATABASE, MEASUREMENT, TAGS, FIELDS);
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        this.serverURL = config.getString(SERVER_URL);
        this.database = config.getString(DATABASE);
        this.measurement = config.getString(MEASUREMENT);
        this.tags = config.getStringList(TAGS);
        this.fields = config.getStringList(FIELDS);
    }
}

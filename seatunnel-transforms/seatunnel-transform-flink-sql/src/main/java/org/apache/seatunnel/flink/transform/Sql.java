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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchTransform;
import org.apache.seatunnel.flink.stream.FlinkStreamTransform;
import org.apache.seatunnel.flink.util.TableUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Sql implements FlinkStreamTransform<Row, Row>, FlinkBatchTransform<Row, Row> {

    private String sql;

    private Config config;

    private static final String SQL = "sql";

    @Override
    public DataStream<Row> processStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        StreamTableEnvironment tableEnvironment = env.getStreamTableEnvironment();
        Table table = tableEnvironment.sqlQuery(sql);
        return TableUtil.tableToDataStream(tableEnvironment, table, false);
    }

    @Override
    public DataSet<Row> processBatch(FlinkEnvironment env, DataSet<Row> data) {
        BatchTableEnvironment tableEnvironment = env.getBatchTableEnvironment();
        Table table = tableEnvironment.sqlQuery(sql);
        return TableUtil.tableToDataSet(tableEnvironment, table);
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
        return CheckConfigUtil.checkAllExists(config, SQL);
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        sql = config.getString("sql");
    }
}

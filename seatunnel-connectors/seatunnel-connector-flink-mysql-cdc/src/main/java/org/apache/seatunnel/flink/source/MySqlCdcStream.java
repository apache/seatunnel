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

package org.apache.seatunnel.flink.source;

import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.stream.FlinkStreamSource;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlCdcStream implements FlinkStreamSource<Row> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlCdcStream.class);
    private static final long serialVersionUID = 334228730117904720L;


    private static final String HOSTNAME = "hostname";
    private static final String PORT = "port";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String DATABASE = "database";
    private static final String TABLE = "table";
    private static final String SCAN_INCREMENTAL_SNAPSHOT_ENABLED = "scan_incremental_snapshot_enabled";
    private Config config;

    @Override
    public Config getConfig() {
        LOGGER.warn("Enter getConfig");
        return config;
    }

    @Override
    public void setConfig(Config config) {
        LOGGER.warn("Enter setConfig");
        this.config = config;
    }

    @Override
    public CheckResult checkConfig() {
        LOGGER.warn("Enter checkConfig");
        return CheckConfigUtil.checkAllExists(config, "hostname", "port", "username", "password", "database", "table", "scan_incremental_snapshot_enabled");
    }

    @Override
    public void prepare(FlinkEnvironment prepareEnv) {
        LOGGER.warn("Enter prepare");
    }

    @SuppressWarnings("checkstyle:CommentsIndentation")
    @Override
    public DataStream<Row> getData(FlinkEnvironment env) {
        LOGGER.warn("Enter getData");
        StreamTableEnvironment tableEnv = env.getStreamTableEnvironment();
        tableEnv.executeSql(
            "create table source ("
                + "column_1 int"
                + ") with ("
                + "'connector' = 'mysql-cdc',"
                + "'scan.incremental.snapshot.enabled' = 'false',"
                + "'hostname' = '192.168.137.200',"
                + " 'port' = '3306',"
                + "'username' = 'root',"
                + "'password' = '141164',"
                + "'database-name' = 'tpcc',"
                + "'table-name' = 'source')");

        Table table = tableEnv.sqlQuery("select * from source");
        DataStream<Row> resultStream = tableEnv.toDataStream(table);
        return resultStream;
    }
}

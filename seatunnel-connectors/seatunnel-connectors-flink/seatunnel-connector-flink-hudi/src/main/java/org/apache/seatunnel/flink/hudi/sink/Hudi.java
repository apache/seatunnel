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

package org.apache.seatunnel.flink.hudi.sink;

import static org.apache.seatunnel.flink.hudi.sink.Config.HUDI_NATIVE_CONFIG_PREFIX;
import static org.apache.seatunnel.flink.hudi.sink.Config.HUDI_STREAM_SINK_TABLE_NAME;
import static org.apache.seatunnel.flink.hudi.sink.Config.HUDI_STREAM_SINK_TABLE_PATH;
import static org.apache.hudi.configuration.FlinkOptions.PATH;
import static org.apache.hudi.configuration.FlinkOptions.TABLE_NAME;

import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.stream.FlinkStreamSink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Hudi implements FlinkStreamSink {
    private static final Logger LOGGER = LoggerFactory.getLogger(Hudi.class);
    private static final String CREATE_SQL = "create temporary table %s  (" +
            "%s" +
            ") WITH (\n" +
            "    'connector' = 'hudi'\n ," +
            "    'path' = '%s'\n " +
            ")";

    private Config config;
    private Configuration flinkConf;
    private String sourceTableName;
    private String tableName;
    private String path;

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
        return CheckConfigUtil.checkAllExists(config, HUDI_STREAM_SINK_TABLE_NAME, HUDI_STREAM_SINK_TABLE_PATH, SOURCE_TABLE_NAME);
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        this.tableName = config.getString(HUDI_STREAM_SINK_TABLE_NAME);
        this.path = config.getString(HUDI_STREAM_SINK_TABLE_PATH);

        final Set<Map.Entry<String, ConfigValue>> entries = config.entrySet();
        final Map<String, String> map = new HashMap<>(entries.size());
        for (Map.Entry<String, ConfigValue> entry : entries) {
            if (entry.getKey().startsWith(HUDI_NATIVE_CONFIG_PREFIX)) {
                map.put(entry.getKey(), String.valueOf(entry.getValue().unwrapped()));
            }
        }

        this.flinkConf = Configuration.fromMap(map);
        flinkConf.setString(TABLE_NAME, tableName);
        flinkConf.setString(PATH, path);

        this.sourceTableName = config.getString(SOURCE_TABLE_NAME);
    }

    @Override
    public String getPluginName() {
        return "Hudi";
    }

    @Override
    @SuppressWarnings("magicnumber")
    public void outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {

        final StreamTableEnvironment tEnv = env.getStreamTableEnvironment();
        final TableConfig config = tEnv.getConfig();
        config.getConfiguration().addAll(flinkConf);

        final CloseableIterator<Row> describeTransformTable = tEnv.executeSql(String.format("DESCRIBE %s", sourceTableName)).collect();

        List<String> list = new ArrayList<>();
        while (describeTransformTable.hasNext()){
            final Row row = describeTransformTable.next();
            list.add(String.format("%s %s ,", row.getField(0), row.getField(1)));
        }
        StringBuilder sb = new StringBuilder();
        list.forEach(sb::append);
        final String allFields = sb.deleteCharAt(sb.length() - 1).toString();

        final String insertSql = BuildInsertSql.builder()
                // only support append now.
                .withAppend()
                .withSinkTable(tableName)
                .withSelect(null)
                .withFrom(sourceTableName)
                .build();

        tEnv.executeSql(String.format(CREATE_SQL, tableName, allFields, path));
        tEnv.executeSql(insertSql);
    }
}

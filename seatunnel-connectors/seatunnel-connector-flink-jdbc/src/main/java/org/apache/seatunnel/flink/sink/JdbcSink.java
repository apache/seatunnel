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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSink;
import org.apache.seatunnel.flink.stream.FlinkStreamSink;
import org.apache.seatunnel.flink.util.SchemaUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

public class JdbcSink implements FlinkStreamSink<Row, Row>, FlinkBatchSink<Row, Row> {

    private Config config;
    private String driverName;
    private String dbUrl;
    private String username;
    private String password;
    private String query;
    private int batchSize = 5000;

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
        return CheckConfigUtil.check(config, "driver", "url", "username", "query");
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        driverName = config.getString("driver");
        dbUrl = config.getString("url");
        username = config.getString("username");
        query = config.getString("query");
        if (config.hasPath("password")) {
            password = config.getString("password");
        }
        if (config.hasPath("batch_size")) {
            batchSize = config.getInt("batch_size");
        }
    }

    @Override
    public DataStreamSink<Row> outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        Table table = env.getStreamTableEnvironment().fromDataStream(dataStream);
        createSink(env.getStreamTableEnvironment(), table);
        return null;
    }

    @Override
    public DataSink<Row> outputBatch(FlinkEnvironment env, DataSet<Row> dataSet) {
        final Table table = env.getBatchTableEnvironment().fromDataSet(dataSet);
        createSink(env.getBatchTableEnvironment(), table);
        return null;
    }

    private void createSink(TableEnvironment tableEnvironment, Table table) {
        TypeInformation<?>[] fieldTypes = table.getSchema().getFieldTypes();
        String[] fieldNames = table.getSchema().getFieldNames();
        TableSink sink = JDBCAppendTableSink.builder()
                .setDrivername(driverName)
                .setDBUrl(dbUrl)
                .setUsername(username)
                .setPassword(password)
                .setBatchSize(batchSize)
                .setQuery(query)
                .setParameterTypes(fieldTypes)
                .build()
                .configure(fieldNames, fieldTypes);
        String uniqueTableName = SchemaUtil.getUniqueTableName();
        tableEnvironment.registerTableSink(uniqueTableName, sink);
        table.insertInto(uniqueTableName);
    }
}

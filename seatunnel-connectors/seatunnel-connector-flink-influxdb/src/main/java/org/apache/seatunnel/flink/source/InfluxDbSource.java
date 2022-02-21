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

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BOOLEAN_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.DOUBLE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.FLOAT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;

import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSource;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;

public class InfluxDbSource implements FlinkBatchSource<Row> {

    private Config config;
    private InfluxDbInputFormat influxDbInputFormat;

    private static final String SERVER_URL = "server_url";
    private static final String USER_NAME = "username";
    private static final String PASSWORD = "password";
    private static final String DATABASE = "database";
    private static final String MEASUREMENT = "measurement";
    private static final String FIELDS = "fields";
    private static final String FIELD_TYPES = "field_types";
    private static final String START_TIMESTAMP = "start_date";
    private static final String END_TIMESTAMP = "end_date";
    private static final String PARALLELISM = "parallelism";

    private HashMap<String, TypeInformation> informationMapping = new HashMap<>();

    {
        // https://docs.influxdata.com/influxdb/v1.8/query_language/explore-data/#data-types
        informationMapping.put("FLOAT", FLOAT_TYPE_INFO);
        informationMapping.put("DOUBLE", DOUBLE_TYPE_INFO);
        informationMapping.put("INTEGER", INT_TYPE_INFO);
        informationMapping.put("LONG", LONG_TYPE_INFO);
        informationMapping.put("STRING", STRING_TYPE_INFO);
        informationMapping.put("BOOLEAN", BOOLEAN_TYPE_INFO);
        informationMapping.put("TIMESTAMP", SqlTimeTypeInfo.TIMESTAMP);
        informationMapping.put("DATE", SqlTimeTypeInfo.DATE);
    }

    @Override
    public DataSet<Row> getData(FlinkEnvironment env) {
        DataSource<Row> dataSource = env.getBatchEnvironment().createInput(influxDbInputFormat);
        if (config.hasPath(PARALLELISM)) {
            int parallelism = config.getInt(PARALLELISM);
            return dataSource.setParallelism(parallelism);
        }
        return dataSource;
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
        return CheckConfigUtil.check(config, SERVER_URL, DATABASE, MEASUREMENT, FIELDS, FIELD_TYPES);
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        String serverURL = config.getString(SERVER_URL);
        String username = config.hasPath(USER_NAME) ? config.getString(USER_NAME) : null;
        String password = config.hasPath(PASSWORD) ? config.getString(PASSWORD) : null;
        String database = config.getString(DATABASE);
        String measurement = config.getString(MEASUREMENT);
        List<String> fields = config.getStringList(FIELDS);
        List<String> fieldTypes = config.getStringList(FIELD_TYPES);
        if (CollectionUtils.isEmpty(fields)) {
            throw new IllegalArgumentException("The fields should not be empty!");
        }
        if (fields.size() != fieldTypes.size()) {
            throw new IllegalArgumentException("The fields and field_types should have same size!");
        }
        String startTimestamp = config.hasPath(START_TIMESTAMP) ? config.getString(START_TIMESTAMP) : null;
        String endTimestamp = config.hasPath(END_TIMESTAMP) ? config.getString(END_TIMESTAMP) : null;

        String sql = new InfluxDbSql(measurement, fields, startTimestamp, endTimestamp).sql();

        this.influxDbInputFormat = InfluxDbInputFormat.buildInfluxDbInputFormat()
                .setServerURL(serverURL)
                .setUsername(username)
                .setPassword(password)
                .setDatabase(database)
                .setQuery(sql)
                .setFields(fields)
                .setRowTypeInfo(getRowTypeInfo(fields, fieldTypes))
                .finish();
    }

    private RowTypeInfo getRowTypeInfo(List<String> fields, List<String> fieldTypes) {
        TypeInformation<?>[] typeInformation = new TypeInformation<?>[fieldTypes.size()];
        String[] names = new String[fieldTypes.size()];
        for (int i = 0; i < fields.size(); i++) {
            typeInformation[i] = this.informationMapping.get(fieldTypes.get(i));
            names[i] = fields.get(i);
        }
        return new RowTypeInfo(typeInformation, names);
    }
}

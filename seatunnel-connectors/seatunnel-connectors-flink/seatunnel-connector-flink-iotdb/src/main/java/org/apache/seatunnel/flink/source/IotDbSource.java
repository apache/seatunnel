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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;

public class IotDbSource implements FlinkBatchSource<Row> {

    private Config config;
    private IotDbInputFormat ioTDbInputFormat;

    private static final String URL = "url";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String STORAGE = "storage";
    private static final String FIELDS = "fields";
    private static final String FIELD_TYPES = "field_types";
    private static final String START_TIMESTAMP = "start_timestamp";
    private static final String END_TIMESTAMP = "end_timestamp";

    private HashMap<String, TypeInformation> informationMapping = new HashMap<>();

    {
        // https://iotdb.apache.org/UserGuide/Master/Data-Concept/Data-Type.html
        // BOOLEAN (Boolean)
        // INT32 (Integer)
        // INT64 (Long Integer)
        // FLOAT (Single Precision Floating Point)
        // DOUBLE (Double Precision Floating Point)
        // TEXT (String)
        informationMapping.put("BOOLEAN", BOOLEAN_TYPE_INFO);
        informationMapping.put("INT32", INT_TYPE_INFO);
        informationMapping.put("INT64", LONG_TYPE_INFO);
        informationMapping.put("FLOAT", FLOAT_TYPE_INFO);
        informationMapping.put("DOUBLE", DOUBLE_TYPE_INFO);
        informationMapping.put("TEXT", STRING_TYPE_INFO);
    }

    @Override
    public DataSet<Row> getData(FlinkEnvironment env) {
        return env.getBatchEnvironment().createInput(ioTDbInputFormat);
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
        return CheckConfigUtil.check(config, URL, STORAGE, FIELDS, FIELD_TYPES);
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        String url = config.getString(URL);
        String user = config.hasPath(USER) ? config.getString(USER) : null;
        String password = config.hasPath(PASSWORD) ? config.getString(PASSWORD) : null;
        String storage = config.getString(STORAGE);
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

        String sql = new IotDbSql(storage, fields, startTimestamp, endTimestamp).sql();

        this.ioTDbInputFormat = IotDbInputFormat.buildIoTDbInputFormat()
                .setURL(url)
                .setUser(user)
                .setPassword(password)
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

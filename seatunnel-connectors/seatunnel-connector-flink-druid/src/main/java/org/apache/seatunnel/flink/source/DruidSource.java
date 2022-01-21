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

import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSource;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BIG_DEC_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BOOLEAN_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BYTE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.DOUBLE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.FLOAT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.SHORT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;

public class DruidSource implements FlinkBatchSource<Row> {

    private Config config;
    private DruidInputFormat druidInputFormat;

    private static final String JDBC_URL = "jdbc_url";
    private static final String DATASOURCE = "datasource";
    private static final String START_TIMESTAMP = "start_date";
    private static final String END_TIMESTAMP = "end_date";
    private static final String COLUMNS = "columns";

    private HashMap<String, TypeInformation> informationMapping = new HashMap<>();

    {
        // https://druid.apache.org/docs/latest/querying/sql.html#data-types
        informationMapping.put("CHAR", STRING_TYPE_INFO);
        informationMapping.put("VARCHAR", STRING_TYPE_INFO);
        informationMapping.put("DECIMAL", BIG_DEC_TYPE_INFO);
        informationMapping.put("FLOAT", FLOAT_TYPE_INFO);
        informationMapping.put("REAL", DOUBLE_TYPE_INFO);
        informationMapping.put("DOUBLE", DOUBLE_TYPE_INFO);
        informationMapping.put("BOOLEAN", BOOLEAN_TYPE_INFO);
        informationMapping.put("TINYINT", BYTE_TYPE_INFO);
        informationMapping.put("SMALLINT", SHORT_TYPE_INFO);
        informationMapping.put("INTEGER", INT_TYPE_INFO);
        informationMapping.put("BIGINT", LONG_TYPE_INFO);
        informationMapping.put("TIMESTAMP", SqlTimeTypeInfo.TIMESTAMP);
        informationMapping.put("DATE", SqlTimeTypeInfo.DATE);
    }

    @Override
    public DataSet<Row> getData(FlinkEnvironment env) {
        return env.getBatchEnvironment().createInput(druidInputFormat);
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
        return CheckConfigUtil.checkAllExists(config, JDBC_URL, DATASOURCE);
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        String jdbcURL = config.getString(JDBC_URL);
        String datasource = config.getString(DATASOURCE);
        String startTimestamp = config.hasPath(START_TIMESTAMP) ? config.getString(START_TIMESTAMP) : null;
        String endTimestamp = config.hasPath(END_TIMESTAMP) ? config.getString(END_TIMESTAMP) : null;
        List<String> columns = config.hasPath(COLUMNS) ? config.getStringList(COLUMNS) : null;

        String sql = new DruidSql(datasource, startTimestamp, endTimestamp, columns).sql();

        this.druidInputFormat = DruidInputFormat.buildDruidInputFormat()
                .setDBUrl(jdbcURL)
                .setQuery(sql)
                .setRowTypeInfo(getRowTypeInfo(jdbcURL, datasource, columns))
                .finish();
    }

    private RowTypeInfo getRowTypeInfo(String jdbcURL, String datasource, Collection<String> userColumns) {
        HashMap<String, TypeInformation> map = new LinkedHashMap<>();

        try (Connection connection = DriverManager.getConnection(jdbcURL)) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet columns = metaData.getColumns(connection.getCatalog(), connection.getSchema(), datasource, "%");
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                String dataTypeName = columns.getString("TYPE_NAME");
                if (userColumns == null || userColumns.contains(columnName)) {
                    map.put(columnName, informationMapping.get(dataTypeName));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        int size = map.size();
        if (userColumns != null && userColumns.size() > 0) {
            size = userColumns.size();
        } else {
            userColumns = map.keySet();
        }

        TypeInformation<?>[] typeInformation = new TypeInformation<?>[size];
        String[] names = new String[size];
        int i = 0;

        for (String field : userColumns) {
            typeInformation[i] = map.get(field);
            names[i] = field;
            i++;
        }
        return new RowTypeInfo(typeInformation, names);
    }
}

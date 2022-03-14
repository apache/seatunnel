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

import static org.apache.seatunnel.flink.Config.DRIVER;
import static org.apache.seatunnel.flink.Config.PARALLELISM;
import static org.apache.seatunnel.flink.Config.PASSWORD;
import static org.apache.seatunnel.flink.Config.QUERY;
import static org.apache.seatunnel.flink.Config.SOURCE_FETCH_SIZE;
import static org.apache.seatunnel.flink.Config.URL;
import static org.apache.seatunnel.flink.Config.USERNAME;

import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSource;
import org.apache.seatunnel.flink.jdbc.input.DefaultTypeInformationMap;
import org.apache.seatunnel.flink.jdbc.input.JdbcInputFormat;
import org.apache.seatunnel.flink.jdbc.input.MysqlTypeInformationMap;
import org.apache.seatunnel.flink.jdbc.input.PostgresTypeInformationMap;
import org.apache.seatunnel.flink.jdbc.input.TypeInformationMap;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JdbcSource implements FlinkBatchSource<Row> {

    private static final long serialVersionUID = -3349505356339446415L;
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSource.class);
    private static final int DEFAULT_FETCH_SIZE = 10000;

    private Config config;
    private String tableName;
    private String driverName;
    private String dbUrl;
    private String username;
    private String password;
    private int fetchSize = DEFAULT_FETCH_SIZE;
    private Set<String> fields;

    private static final Pattern COMPILE = Pattern.compile("select (.+) from (.+).*");

    private JdbcInputFormat jdbcInputFormat;

    @Override
    public DataSet<Row> getData(FlinkEnvironment env) {
        DataSource<Row> dataSource = env.getBatchEnvironment().createInput(jdbcInputFormat);
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
        return CheckConfigUtil.checkAllExists(config, DRIVER, URL, USERNAME, QUERY);
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        driverName = config.getString(DRIVER);
        dbUrl = config.getString(URL);
        username = config.getString(USERNAME);
        String query = config.getString(QUERY);
        Matcher matcher = COMPILE.matcher(query);
        if (matcher.find()) {
            String var = matcher.group(1);
            tableName = matcher.group(2);
            if (!"*".equals(var.trim())) {
                LinkedHashSet<String> vars = new LinkedHashSet<>();
                String[] split = var.split(",");
                for (String s : split) {
                    vars.add(s.trim());
                }
                fields = vars;
            }
        }
        if (config.hasPath(PASSWORD)) {
            password = config.getString(PASSWORD);
        }
        if (config.hasPath(SOURCE_FETCH_SIZE)) {
            fetchSize = config.getInt(SOURCE_FETCH_SIZE);
        }

        jdbcInputFormat = JdbcInputFormat.buildFlinkJdbcInputFormat().setDrivername(driverName).setDBUrl(dbUrl).setUsername(username).setPassword(password).setQuery(query).setFetchSize(fetchSize).setRowTypeInfo(getRowTypeInfo()).finish();
    }

    private RowTypeInfo getRowTypeInfo() {
        Map<String, TypeInformation<?>> map = new LinkedHashMap<>();

        try {
            Class.forName(driverName);
            TypeInformationMap informationMapping = getTypeInformationMap(driverName);
            Connection connection = DriverManager.getConnection(dbUrl, username, password);
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet columns = metaData.getColumns(connection.getCatalog(), connection.getSchema(), tableName, "%");
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                String dataTypeName = columns.getString("TYPE_NAME");
                if (fields == null || fields.contains(columnName)) {
                    map.put(columnName, informationMapping.getInformation(dataTypeName));
                }
            }
            connection.close();
        } catch (Exception e) {
            LOGGER.warn("get row type info exception", e);
        }

        int size = map.size();
        if (fields != null && fields.size() > 0) {
            size = fields.size();
        } else {
            fields = map.keySet();
        }

        TypeInformation<?>[] typeInformation = new TypeInformation<?>[size];
        String[] names = new String[size];
        int i = 0;

        for (String field : fields) {
            typeInformation[i] = map.get(field);
            names[i] = field;
            i++;
        }
        return new RowTypeInfo(typeInformation, names);
    }

    private TypeInformationMap getTypeInformationMap(String driverName) {
        driverName = driverName.toLowerCase();
        if (driverName.contains("mysql")) {
            return new MysqlTypeInformationMap();
        } else if (driverName.contains("postgresql")) {
            return new PostgresTypeInformationMap();
        } else {
            return new DefaultTypeInformationMap();
        }
    }

}

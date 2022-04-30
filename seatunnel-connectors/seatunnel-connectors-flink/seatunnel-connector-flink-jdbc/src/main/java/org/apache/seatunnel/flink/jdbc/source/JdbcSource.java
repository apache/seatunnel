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

package org.apache.seatunnel.flink.jdbc.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.split.JdbcNumericBetweenParametersProvider;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;
import org.apache.flink.types.Row;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSource;
import org.apache.seatunnel.flink.jdbc.input.*;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.*;
import static org.apache.seatunnel.flink.jdbc.Config.*;

public class JdbcSource implements FlinkBatchSource {

    private static final long serialVersionUID = -3349505356339446415L;
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSource.class);
    private static final int DEFAULT_FETCH_SIZE = 10000;

    private Config config;
    private String password;
    private int fetchSize = DEFAULT_FETCH_SIZE;
    private int parallelism = -1;
    private Map<String, TypeInformation<?>> tableFieldInfo;

    private static final Pattern COMPILE = Pattern.compile("[\\s]*select[\\s]*(.*)from[\\s]*([\\S]+)(.*)",
            Pattern.CASE_INSENSITIVE);

    private JdbcInputFormat jdbcInputFormat;

    @Override
    public DataSet<Row> getData(FlinkEnvironment env) {
        DataSource<Row> dataSource = env.getBatchEnvironment().createInput(jdbcInputFormat);
        if (config.hasPath(PARALLELISM)) {
            return dataSource.setParallelism(config.getInt(PARALLELISM));
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
        String driverName = config.getString(DRIVER);
        String dbUrl = config.getString(URL);
        String username = config.getString(USERNAME);
        String query = config.getString(QUERY);

        if (config.hasPath(PASSWORD)) {
            password = config.getString(PASSWORD);
        }
        if (config.hasPath(SOURCE_FETCH_SIZE)) {
            fetchSize = config.getInt(SOURCE_FETCH_SIZE);
        }
        if (config.hasPath(PARALLELISM)) {
            parallelism = config.getInt(PARALLELISM);
        } else {
            parallelism = env.getBatchEnvironment().getParallelism();
        }
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("jdbc connection init failed.", e);
        }

        try (Connection connection = DriverManager.getConnection(dbUrl, username, password)) {
            tableFieldInfo = initTableField(connection, query);
            RowTypeInfo rowTypeInfo = getRowTypeInfo();
            JdbcInputFormat.JdbcInputFormatBuilder builder = JdbcInputFormat.buildFlinkJdbcInputFormat();
            if (config.hasPath(PARTITION_COLUMN)) {
                String partitionColumn = config.getString(PARTITION_COLUMN);
                if (!tableFieldInfo.containsKey(partitionColumn)) {
                    throw new IllegalArgumentException(String.format("field %s not contain in query sql %s",
                            partitionColumn, query));
                }
                if (!isNumericType(rowTypeInfo.getTypeAt(partitionColumn))) {
                    throw new IllegalArgumentException(String.format("%s is not numeric type", partitionColumn));
                }
                JdbcParameterValuesProvider jdbcParameterValuesProvider =
                        initPartition(partitionColumn, connection, query);
                builder.setParametersProvider(jdbcParameterValuesProvider);
                query = String.format("SELECT * FROM (%s) tt where " + partitionColumn + " >= ? AND " + partitionColumn + " < ?", query);
            }
            builder.setDrivername(driverName).setDBUrl(dbUrl).setUsername(username)
                    .setPassword(password).setQuery(query).setFetchSize(fetchSize)
                    .setRowTypeInfo(rowTypeInfo);
            jdbcInputFormat = builder.finish();
        } catch (SQLException e) {
            throw new RuntimeException("jdbc connection init failed.", e);
        }
    }

    @Override
    public String getPluginName() {
        return "JdbcSource";
    }

    private JdbcParameterValuesProvider initPartition(String columnName, Connection connection, String query) throws SQLException {
        long max = Long.MAX_VALUE;
        long min = Long.MIN_VALUE;
        if (config.hasPath(PARTITION_UPPER_BOUND) && config.hasPath(PARTITION_LOWER_BOUND)) {
            max = config.getLong(PARTITION_UPPER_BOUND);
            min = config.getLong(PARTITION_LOWER_BOUND);
            return new JdbcNumericBetweenParametersProvider(min, max).ofBatchNum(parallelism * 2);
        }
        try (ResultSet rs = connection.createStatement().executeQuery(String.format("SELECT MAX(%s),MIN(%s) " +
                "FROM (%s) tt", columnName, columnName, query))) {
            if (rs.next()) {
                max = config.hasPath(PARTITION_UPPER_BOUND) ? config.getLong(PARTITION_UPPER_BOUND) :
                        Long.parseLong(rs.getString(1));
                min = config.hasPath(PARTITION_LOWER_BOUND) ? config.getLong(PARTITION_LOWER_BOUND) :
                        Long.parseLong(rs.getString(2));
            }
        }
        return new JdbcNumericBetweenParametersProvider(min, max).ofBatchNum(parallelism * 2);
    }

    private boolean isNumericType(TypeInformation<?> type) {
        return type.equals(INT_TYPE_INFO) || type.equals(SHORT_TYPE_INFO)
                || type.equals(LONG_TYPE_INFO) || type.equals(BIG_INT_TYPE_INFO);
    }

    private Map<String, TypeInformation<?>> initTableField(Connection connection, String selectSql) {
        try {
            String databaseDialect = connection.getMetaData().getDatabaseProductName();
            PreparedStatement preparedStatement = connection.prepareStatement(selectSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            preparedStatement.setMaxRows(1);
            ResultSetMetaData rsMeta = preparedStatement.getMetaData();
            try {
                return getRowInfo(rsMeta, databaseDialect);
            } catch (SQLException e) {
                ResultSet rs = preparedStatement.executeQuery();
                return getRowInfo(rs.getMetaData(), databaseDialect);
            }
        } catch (SQLException e) {
            LOGGER.warn("get row type info exception", e);
        }
        return new LinkedHashMap<>();
    }

    private Map<String, TypeInformation<?>> getRowInfo(ResultSetMetaData rsMeta, String databaseDialect) throws SQLException {
        Map<String, TypeInformation<?>> map = new LinkedHashMap<>();
        if (rsMeta == null) {
            throw new SQLException("No result set metadata available to resolver row info!");
        }
        TypeInformationMap informationMapping = getTypeInformationMap(databaseDialect);
        for (int i = 1; i < rsMeta.getColumnCount(); i++) {
            String columnName = rsMeta.getColumnLabel(i);
            String columnTypeName = rsMeta.getColumnTypeName(i);
            if (columnTypeName == null) {
                throw new SQLException("Unsupported to get type info from result set metadata!");
            }
            map.put(columnName, informationMapping.getInformation(columnTypeName));
        }
        return map;
    }

    private RowTypeInfo getRowTypeInfo() {
        int size = tableFieldInfo.size();
        Set<String> fields = tableFieldInfo.keySet();
        TypeInformation<?>[] typeInformation = new TypeInformation<?>[size];
        String[] names = new String[size];
        int i = 0;
        for (String field : fields) {
            typeInformation[i] = tableFieldInfo.get(field);
            names[i] = field;
            i++;
        }
        return new RowTypeInfo(typeInformation, names);
    }

    private TypeInformationMap getTypeInformationMap(String databaseDialect) {
        if ("mysql".equalsIgnoreCase(databaseDialect)) {
            return new MysqlTypeInformationMap();
        } else if ("postgresql".equalsIgnoreCase(databaseDialect)) {
            return new PostgresTypeInformationMap();
        } else if ("oracle".equalsIgnoreCase(databaseDialect)) {
            return new OracleTypeInformationMap();
        } else {
            return new DefaultTypeInformationMap();
        }
    }

}

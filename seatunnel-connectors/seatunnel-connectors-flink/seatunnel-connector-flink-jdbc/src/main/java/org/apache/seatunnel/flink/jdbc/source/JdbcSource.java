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

import static org.apache.seatunnel.flink.jdbc.Config.DRIVER;
import static org.apache.seatunnel.flink.jdbc.Config.PARALLELISM;
import static org.apache.seatunnel.flink.jdbc.Config.PARTITION_COLUMN;
import static org.apache.seatunnel.flink.jdbc.Config.PARTITION_LOWER_BOUND;
import static org.apache.seatunnel.flink.jdbc.Config.PARTITION_UPPER_BOUND;
import static org.apache.seatunnel.flink.jdbc.Config.PASSWORD;
import static org.apache.seatunnel.flink.jdbc.Config.QUERY;
import static org.apache.seatunnel.flink.jdbc.Config.SOURCE_FETCH_SIZE;
import static org.apache.seatunnel.flink.jdbc.Config.URL;
import static org.apache.seatunnel.flink.jdbc.Config.USERNAME;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BIG_INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.SHORT_TYPE_INFO;

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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.split.JdbcNumericBetweenParametersProvider;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JdbcSource implements FlinkBatchSource {

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
    private int parallelism = -1;
    private Set<String> fields;
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
        driverName = config.getString(DRIVER);
        dbUrl = config.getString(URL);
        username = config.getString(USERNAME);
        String query = config.getString(QUERY);
        Tuple2<String, Set<String>> tableNameAndFields = getTableNameAndFields(COMPILE, query);
        tableName = tableNameAndFields.f0;
        fields = tableNameAndFields.f1;
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
            tableFieldInfo = initTableField(connection);
            RowTypeInfo rowTypeInfo = getRowTypeInfo();
            JdbcInputFormat.JdbcInputFormatBuilder builder = JdbcInputFormat.buildFlinkJdbcInputFormat();
            if (config.hasPath(PARTITION_COLUMN)) {
                if (!tableFieldInfo.containsKey(config.getString(PARTITION_COLUMN))) {
                    throw new IllegalArgumentException(String.format("field %s not contain in table %s",
                            config.getString(PARTITION_COLUMN), tableName));
                }
                if (!isNumericType(rowTypeInfo.getTypeAt(config.getString(PARTITION_COLUMN)))) {
                    throw new IllegalArgumentException(String.format("%s is not numeric type", PARTITION_COLUMN));
                }
                JdbcParameterValuesProvider jdbcParameterValuesProvider =
                        initPartition(config.getString(PARTITION_COLUMN), connection);
                builder.setParametersProvider(jdbcParameterValuesProvider);
                query = extendPartitionQuerySql(query, config.getString(PARTITION_COLUMN));
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

    private String extendPartitionQuerySql(String query, String column) {
        Matcher matcher = COMPILE.matcher(query);
        if (matcher.find()) {
            String where = matcher.group(Integer.parseInt("3"));
            if (where != null && where.trim().toLowerCase().startsWith("where")) {
                // contain where
                return query + " AND \"" + column + "\" BETWEEN ? AND ?";
            } else {
                // not contain where
                return query + " WHERE \"" + column + "\" BETWEEN ? AND ?";
            }
        } else {
            throw new IllegalArgumentException("sql statement format is incorrect :" + query);
        }
    }

    private JdbcParameterValuesProvider initPartition(String columnName, Connection connection) throws SQLException {
        long max = Long.MAX_VALUE;
        long min = Long.MIN_VALUE;
        if (config.hasPath(PARTITION_UPPER_BOUND) && config.hasPath(PARTITION_LOWER_BOUND)) {
            max = config.getLong(PARTITION_UPPER_BOUND);
            min = config.getLong(PARTITION_LOWER_BOUND);
            return new JdbcNumericBetweenParametersProvider(min, max).ofBatchNum(parallelism * 2);
        }
        try (ResultSet rs = connection.createStatement().executeQuery(String.format("SELECT MAX(%s),MIN(%s) " +
                "FROM %s", columnName, columnName, tableName))) {
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

    private Map<String, TypeInformation<?>> initTableField(Connection connection) {
        Map<String, TypeInformation<?>> map = new LinkedHashMap<>();

        try {
            TypeInformationMap informationMapping = getTypeInformationMap(driverName);
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet columns = metaData.getColumns(connection.getCatalog(), connection.getSchema(), tableName, "%");
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                String dataTypeName = columns.getString("TYPE_NAME");
                if (fields == null || fields.contains(columnName)) {
                    map.put(columnName, informationMapping.getInformation(dataTypeName));
                }
            }
        } catch (Exception e) {
            LOGGER.warn("get row type info exception", e);
        }
        return map;
    }

    private Tuple2<String, Set<String>> getTableNameAndFields(Pattern regex, String selectSql) {
        Matcher matcher = regex.matcher(selectSql);
        String tableName;
        Set<String> fields = null;
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
            return new Tuple2<>(tableName, fields);
        } else {
            throw new IllegalArgumentException("can't find tableName and fields in sql :" + selectSql);
        }
    }

    private RowTypeInfo getRowTypeInfo() {
        int size = tableFieldInfo.size();
        if (fields != null && fields.size() > 0) {
            size = fields.size();
        } else {
            fields = tableFieldInfo.keySet();
        }

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

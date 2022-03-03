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

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BIG_DEC_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BIG_INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BOOLEAN_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BYTE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.DOUBLE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.FLOAT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.SHORT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;

import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSource;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JdbcSource implements FlinkBatchSource<Row> {

    private static final long serialVersionUID = -3349505356339446415L;
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSource.class);

    private Config config;
    private String tableName;
    private String driverName;
    private String dbUrl;
    private String username;
    private String password;
    private int fetchSize = Integer.MIN_VALUE;
    private Set<String> fields;

    private static final Pattern COMPILE = Pattern.compile("select (.+) from (.+).*");
    private static final String PARALLELISM = "parallelism";

    private HashMap<String, TypeInformation> informationMapping = new HashMap<>();

    private JdbcInputFormat jdbcInputFormat;

    {
        informationMapping.put("VARCHAR", STRING_TYPE_INFO);
        informationMapping.put("BOOLEAN", BOOLEAN_TYPE_INFO);
        informationMapping.put("TINYINT", BYTE_TYPE_INFO);
        informationMapping.put("TINYINT UNSIGNED", INT_TYPE_INFO);
        informationMapping.put("SMALLINT", SHORT_TYPE_INFO);
        informationMapping.put("SMALLINT UNSIGNED", INT_TYPE_INFO);
        informationMapping.put("INTEGER", INT_TYPE_INFO);
        informationMapping.put("INTEGER UNSIGNED", INT_TYPE_INFO);
        informationMapping.put("MEDIUMINT", INT_TYPE_INFO);
        informationMapping.put("MEDIUMINT UNSIGNED", INT_TYPE_INFO);
        informationMapping.put("INT", INT_TYPE_INFO);
        informationMapping.put("INT UNSIGNED", LONG_TYPE_INFO);
        informationMapping.put("BIGINT", LONG_TYPE_INFO);
        informationMapping.put("BIGINT UNSIGNED", BIG_INT_TYPE_INFO);
        informationMapping.put("FLOAT", FLOAT_TYPE_INFO);
        informationMapping.put("DOUBLE", DOUBLE_TYPE_INFO);
        informationMapping.put("CHAR", STRING_TYPE_INFO);
        informationMapping.put("TEXT", STRING_TYPE_INFO);
        informationMapping.put("LONGTEXT", STRING_TYPE_INFO);
        informationMapping.put("DATE", SqlTimeTypeInfo.DATE);
        informationMapping.put("TIME", SqlTimeTypeInfo.TIME);
        informationMapping.put("DATETIME", SqlTimeTypeInfo.TIMESTAMP);
        informationMapping.put("TIMESTAMP", SqlTimeTypeInfo.TIMESTAMP);
        informationMapping.put("DECIMAL", BIG_DEC_TYPE_INFO);
        informationMapping.put("BINARY", BYTE_PRIMITIVE_ARRAY_TYPE_INFO);

    }

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
        return CheckConfigUtil.checkAllExists(config, "driver", "url", "username", "query");
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        driverName = config.getString("driver");
        dbUrl = config.getString("url");
        username = config.getString("username");
        String query = config.getString("query");
        Matcher matcher = COMPILE.matcher(query);
        if (matcher.find()) {
            String var = matcher.group(1);
            tableName = matcher.group(2);
            if ("*".equals(var.trim())) {
                //do nothing
            } else {
                LinkedHashSet<String> vars = new LinkedHashSet<>();
                String[] split = var.split(",");
                for (String s : split) {
                    vars.add(s.trim());
                }
                fields = vars;
            }
        }
        if (config.hasPath("password")) {
            password = config.getString("password");
        }
        if (config.hasPath("fetch_size")) {
            fetchSize = config.getInt("fetch_size");
        }

        jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername(driverName)
                .setDBUrl(dbUrl)
                .setUsername(username)
                .setPassword(password)
                .setQuery(query)
                .setFetchSize(fetchSize)
                .setRowTypeInfo(getRowTypeInfo())
                .finish();
    }

    private RowTypeInfo getRowTypeInfo() {
        HashMap<String, TypeInformation> map = new LinkedHashMap<>();

        try {
            Class.forName(driverName);
            Connection connection = DriverManager.getConnection(dbUrl, username, password);
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet columns = metaData.getColumns(connection.getCatalog(), connection.getSchema(), tableName, "%");
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                String dataTypeName = columns.getString("TYPE_NAME");
                if (fields == null || fields.contains(columnName)) {
                    map.put(columnName, informationMapping.get(dataTypeName));
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

}

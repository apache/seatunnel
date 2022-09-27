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

package org.apache.seatunnel.flink.druid.source;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BIG_DEC_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BOOLEAN_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BYTE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.DOUBLE_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.FLOAT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.SHORT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;

import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.flink.BaseFlinkSource;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchSource;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
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
import java.util.*;

@AutoService(BaseFlinkSource.class)
public class DruidSource implements FlinkBatchSource {

    private static final long serialVersionUID = 8152628883440481281L;
    private static final Logger LOGGER = LoggerFactory.getLogger(DruidSource.class);

    private Config config;
    private DruidInputFormat druidInputFormat;

    private static final String JDBC_URL = "jdbc_url";
    private static final String DATASOURCE = "datasource";
    private static final String START_TIMESTAMP = "start_date";
    private static final String END_TIMESTAMP = "end_date";
    private static final String COLUMNS = "columns";
    private static final String PARALLELISM = "parallelism";
    private static final String ESCAPE_DELIMITER_KEY = "escape_delimiter";
    private static final String ESCAPE_DELIMITER_DEFAULT = "\u0001";

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
        DataSource<Row> dataSource = env.getBatchEnvironment().createInput(druidInputFormat);
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
        return CheckConfigUtil.checkAllExists(config, JDBC_URL, DATASOURCE);
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        String jdbcURL = config.getString(JDBC_URL);
        String user = config.getString("user");
        String password = config.getString("password");
        String datasource = config.getString(DATASOURCE);
        String escape_delimiter = config.getString(ESCAPE_DELIMITER_KEY);
        if (StringUtils.isBlank(escape_delimiter)){
            escape_delimiter = ESCAPE_DELIMITER_DEFAULT;
        }
        String startTimestamp = config.hasPath(START_TIMESTAMP) ? config.getString(START_TIMESTAMP).replaceAll(escape_delimiter, " ") : null;
        String endTimestamp = config.hasPath(END_TIMESTAMP) ? config.getString(END_TIMESTAMP).replaceAll(escape_delimiter, " ") : null;
        List<String> columns = config.hasPath(COLUMNS) ? config.getStringList(COLUMNS) : null;
        System.out.println(jdbcURL);
        System.out.println(user);
        System.out.println(password);
        String sql = new DruidSql(datasource, startTimestamp, endTimestamp, columns).sql();
        RowTypeInfo rowTypeInfo = getRowTypeInfo(jdbcURL, user, password, datasource, columns);
        this.druidInputFormat = DruidInputFormat.buildDruidInputFormat()
                .setDBUrl(jdbcURL)
                .setDBUser(user)
                .setDBPassword(password)
                .setQuery(sql)
                .setRowTypeInfo(rowTypeInfo)
                .finish();
    }

    @Override
    public String getPluginName() {
        return "DruidSource";
    }

    public RowTypeInfo getRowTypeInfo(String jdbcURL, String user, String password, String datasource, Collection<String> userColumns) {
        HashMap<String, TypeInformation> map = new LinkedHashMap<>();

        try (Connection connection = DriverManager.getConnection(jdbcURL, user, password)) {
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
            LOGGER.warn("Failed to get column information from JDBC URL: {}", jdbcURL, e);
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
        long count = Arrays.stream(typeInformation).filter(t -> null == t).count();
        System.out.println("typeInformation null:::" + count);
        return new RowTypeInfo(typeInformation, names);
    }
}

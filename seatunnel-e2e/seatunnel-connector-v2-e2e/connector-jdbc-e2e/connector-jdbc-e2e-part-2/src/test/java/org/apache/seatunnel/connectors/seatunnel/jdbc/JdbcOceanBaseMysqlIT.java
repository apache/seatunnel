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

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.apache.commons.lang3.tuple.Pair;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcOceanBaseMysqlIT extends AbstractJdbcIT {

    private static final String OCEANBASE_IMAGE = "oceanbase/oceanbase-ce";

    private static final String OCEANBASE_CONTAINER_HOST = "e2e_oceanbaseDb";
    private static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    private static final int PORT = 2881;
    private static final String DATABASE = "seatunnel";
    private static final String URL =
            "jdbc:mysql://"
                    + HOST
                    + ":"
                    + PORT
                    + "/"
                    + DATABASE
                    + "?createDatabaseIfNotExist=true&_enable_convert_real_to_decimal=true";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";
    private static final String SOURCE_TABLE = "e2e_ob_source";
    private static final String SINK_TABLE = "e2e_ob_sink";
    private static final String DRIVER_JAR =
            "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.16/mysql-connector-java-8.0.16.jar";
    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_oceanbase_mysql_source_and_sink.conf");

    private static final String CREATE_SQL =
            "CREATE TABLE %s\n"
                    + " (\n"
                    + "  col1 INT PRIMARY KEY COMMENT '第一列', \n"
                    + "  col2 integer DEFAULT NULL COMMENT '第二列',\n"
                    + "  col3 decimal(10,2) DEFAULT NULL COMMENT '第三列',\n"
                    + "  col4 number DEFAULT NULL COMMENT '第四列',\n"
                    + "  col5 tinyint(4) DEFAULT NULL COMMENT '第五列',\n"
                    + "  col6 smallint(6) DEFAULT NULL COMMENT '第六列',\n"
                    + "  col7 mediumint(9) DEFAULT NULL COMMENT '第七列',\n"
                    + "  col8 bigint(20) DEFAULT NULL COMMENT '第八列',\n"
                    + "  col9 varchar(10) DEFAULT NULL COMMENT '第九列',\n"
                    + "  col10 varchar(10) DEFAULT NULL COMMENT '第十列',\n"
                    + "  col11 varchar(10) DEFAULT NULL COMMENT '第十一列',\n"
                    + "  col12 char(10) NOT NULL DEFAULT '中文字段' COMMENT '第十二列',\n"
                    + "  col13 text DEFAULT NULL COMMENT '第十三列',\n"
                    + "  col14 tinytext DEFAULT NULL COMMENT '第十四列',\n"
                    + "  col15 mediumtext DEFAULT NULL COMMENT '第十五列',\n"
                    + "  col16 longtext DEFAULT NULL COMMENT '第十六列',\n"
                    + "  col17 blob DEFAULT NULL COMMENT '第十七列',\n"
                    + "  col18 tinyblob DEFAULT NULL COMMENT '第十八列',\n"
                    + "  col19 longblob DEFAULT NULL COMMENT '第十九列',\n"
                    + "  col20 mediumblob DEFAULT NULL COMMENT '第二十列',\n"
                    + "  col21 binary(16) DEFAULT NULL COMMENT '第二十一列',\n"
                    + "  col22 varbinary(16) DEFAULT NULL COMMENT '第二十二列',\n"
                    + "  col23 timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '第二十三列',\n"
                    + "  col24 time DEFAULT NULL COMMENT '第二十四列',\n"
                    + "  col25 date DEFAULT NULL COMMENT '第二十五列',\n"
                    + "  col26 datetime DEFAULT NULL COMMENT '第二十六列',\n"
                    + "  col27 year(4) DEFAULT NULL COMMENT '第二十七列'\n"
                    + ")";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String jdbcUrl = String.format(URL, PORT, DATABASE);
        String[] fieldNames = testDataSet.getKey();
        String insertSql = insertTable(DATABASE, SOURCE_TABLE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(OCEANBASE_IMAGE)
                .networkAliases(OCEANBASE_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(PORT)
                .localPort(PORT)
                .jdbcTemplate(URL)
                .jdbcUrl(jdbcUrl)
                .userName(USERNAME)
                .password(PASSWORD)
                .database(DATABASE)
                .sourceTable(SOURCE_TABLE)
                .sinkTable(SINK_TABLE)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .build();
    }

    @Override
    void compareResult() {}

    @Override
    String driverUrl() {
        return DRIVER_JAR;
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {

        String[] fieldNames =
                new String[] {
                    "col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10",
                    "col11", "col12", "col13", "col14", "col15", "col16", "col17", "col18", "col19",
                    "col20", "col21", "col22", "col23", "col24", "col25", "col26", "col27"
                };

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                100 + i,
                                2,
                                3,
                                4,
                                5,
                                6,
                                7,
                                8,
                                "9.1",
                                "10.1",
                                "十一",
                                "十二",
                                "十三",
                                "十四",
                                "十五",
                                "十六",
                                "十七",
                                "十八",
                                "十九",
                                "二十",
                                "二十一",
                                "二十二",
                                "19700101",
                                "00:00:00",
                                "19700101",
                                "19700101",
                                "1970"
                            });

            rows.add(row);
        }

        return Pair.of(fieldNames, rows);
    }

    @Override
    GenericContainer<?> initContainer() {
        GenericContainer<?> container =
                new GenericContainer<>(OCEANBASE_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(OCEANBASE_CONTAINER_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(OCEANBASE_IMAGE)));

        container.setPortBindings(Lists.newArrayList(String.format("%s:%s", PORT, PORT)));

        return container;
    }

    @Override
    protected void createSchemaIfNeeded() {
        String sql = "CREATE DATABASE IF NOT EXISTS " + DATABASE;
        try {
            connection.prepareStatement(sql).executeUpdate();
        } catch (Exception e) {
            throw new SeaTunnelRuntimeException(
                    JdbcITErrorCode.CREATE_TABLE_FAILED, "Fail to execute sql " + sql, e);
        }
    }
}

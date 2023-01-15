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
import org.apache.seatunnel.connectors.seatunnel.jdbc.util.JdbcCompareUtil;

import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class JdbcOceanBasedbIT extends AbstractJdbcIT {
    private static final String DOCKER_IMAGE = "oceanbase/oceanbase-ce";
    private static final String NETWORK_ALIASES = "e2e_oceanbase";
    private static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    private static final int PORT = 2881;
    private static final String DATABASE = "test";
    private static final String URL = "jdbc:mysql://" + HOST + ":" + PORT + "/" + DATABASE + "?createDatabaseIfNotExist=true";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";
    private static final String SOURCE_TABLE = "e2e_ob_source";
    private static final String SINK_TABLE = "e2e_ob_sink";
    private static final String DRIVER_JAR = "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.16/mysql-connector-java-8.0.16.jar";
    private static final String CONFIG_FILE = "/jdbc_oceanbase_source_and_sink.conf";
    private static final String COLUMN_STRING = "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25,col26,col27";

    private static final String DDL_SOURCE = "CREATE TABLE `" + DATABASE + "." + SOURCE_TABLE + "` (\n" +
        "  `col1` int(11) NOT NULL AUTO_INCREMENT COMMENT '第一列', \n" +
        "  `col2` integer DEFAULT NULL COMMENT '第二列',\n" +
        "  `col3` decimal(10,2) DEFAULT NULL COMMENT '第三列',\n" +
        "  `col4` number DEFAULT NULL COMMENT '第四列',\n" +
        "  `col5` tinyint(4) DEFAULT NULL COMMENT '第五列',\n" +
        "  `col6` smallint(6) DEFAULT NULL COMMENT '第六列',\n" +
        "  `col7` mediumint(9) DEFAULT NULL COMMENT '第七列',\n" +
        "  `col8` bigint(20) DEFAULT NULL COMMENT '第八列',\n" +
        "  `col9` float(10,2) DEFAULT NULL COMMENT '第九列',\n" +
        "  `col10` double(10,2) DEFAULT NULL COMMENT '第十列',\n" +
        "  `col11` varchar(10) DEFAULT NULL COMMENT '第十一列',\n" +
        "  `col12` char(10) NOT NULL DEFAULT '中文字段' COMMENT '第十二列',\n" +
        "  `col13` text DEFAULT NULL COMMENT '第十三列',\n" +
        "  `col14` tinytext DEFAULT NULL COMMENT '第十四列',\n" +
        "  `col15` mediumtext DEFAULT NULL COMMENT '第十五列',\n" +
        "  `col16` longtext DEFAULT NULL COMMENT '第十六列',\n" +
        "  `col17` blob DEFAULT NULL COMMENT '第十七列',\n" +
        "  `col18` tinyblob DEFAULT NULL COMMENT '第十八列',\n" +
        "  `col19` longblob DEFAULT NULL COMMENT '第十九列',\n" +
        "  `col20` mediumblob DEFAULT NULL COMMENT '第二十列',\n" +
        "  `col21` binary(16) DEFAULT NULL COMMENT '第二十一 列',\n" +
        "  `col22` varbinary(16) DEFAULT NULL COMMENT '第二十二列',\n" +
        "  `col23` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '第二十三列',\n" +
        "  `col24` time DEFAULT NULL COMMENT '第二十四列',\n" +
        "  `col25` date DEFAULT NULL COMMENT '第二十五列',\n" +
        "  `col26` datetime DEFAULT NULL COMMENT '第二十六列',\n" +
        "  `col27` year(4) DEFAULT NULL COMMENT '第二十七列',\n" +
        "  PRIMARY KEY (`col1`)\n" +
        ") AUTO_INCREMENT = 1 DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'zstd_1.0' REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 0 COMMENT = '"+ SOURCE_TABLE + "'";


    private static final String DDL_SINK = "CREATE TABLE `" + DATABASE + "." + SINK_TABLE + "` (\n" +
        "  `col1` int(11) NOT NULL AUTO_INCREMENT COMMENT '第一列', \n" +
        "  `col2` integer DEFAULT NULL COMMENT '第二列',\n" +
        "  `col3` decimal(10,2) DEFAULT NULL COMMENT '第三列',\n" +
        "  `col4` number DEFAULT NULL COMMENT '第四列',\n" +
        "  `col5` tinyint(4) DEFAULT NULL COMMENT '第五列',\n" +
        "  `col6` smallint(6) DEFAULT NULL COMMENT '第六列',\n" +
        "  `col7` mediumint(9) DEFAULT NULL COMMENT '第七列',\n" +
        "  `col8` bigint(20) DEFAULT NULL COMMENT '第八列',\n" +
        "  `col9` float(10,2) DEFAULT NULL COMMENT '第九列',\n" +
        "  `col10` double(10,2) DEFAULT NULL COMMENT '第十列',\n" +
        "  `col11` varchar(10) DEFAULT NULL COMMENT '第十一列',\n" +
        "  `col12` char(10) NOT NULL DEFAULT '中文字段' COMMENT '第十二列',\n" +
        "  `col13` text DEFAULT NULL COMMENT '第十三列',\n" +
        "  `col14` tinytext DEFAULT NULL COMMENT '第十四列',\n" +
        "  `col15` mediumtext DEFAULT NULL COMMENT '第十五列',\n" +
        "  `col16` longtext DEFAULT NULL COMMENT '第十六列',\n" +
        "  `col17` blob DEFAULT NULL COMMENT '第十七列',\n" +
        "  `col18` tinyblob DEFAULT NULL COMMENT '第十八列',\n" +
        "  `col19` longblob DEFAULT NULL COMMENT '第十九列',\n" +
        "  `col20` mediumblob DEFAULT NULL COMMENT '第二十列',\n" +
        "  `col21` binary(16) DEFAULT NULL COMMENT '第二十一 列',\n" +
        "  `col22` varbinary(16) DEFAULT NULL COMMENT '第二十二列',\n" +
        "  `col23` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '第二十三列',\n" +
        "  `col24` time DEFAULT NULL COMMENT '第二十四列',\n" +
        "  `col25` date DEFAULT NULL COMMENT '第二十五列',\n" +
        "  `col26` datetime DEFAULT NULL COMMENT '第二十六列',\n" +
        "  `col27` year(4) DEFAULT NULL COMMENT '第二十七列',\n" +
        "  PRIMARY KEY (`col1`)\n" +
        ") AUTO_INCREMENT = 1 DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'zstd_1.0' REPLICA_NUM = 1 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 0 COMMENT = '"+ SINK_TABLE + "'";

    private static final String INIT_DATA_SQL = "insert into " + SOURCE_TABLE + " (\n" +
        "`col1` ,\n" +
        "`col2` ,\n" +
        "`col3` ,\n" +
        "`col4` ,\n" +
        "`col5` ,\n" +
        "`col6` ,\n" +
        "`col7` ,\n" +
        "`col8` ,\n" +
        "`col9` ,\n" +
        "`col10`,\n" +
        "`col11`,\n" +
        "`col12`,\n" +
        "`col13`,\n" +
        "`col14`,\n" +
        "`col15`,\n" +
        "`col16`,\n" +
        "`col17`,\n" +
        "`col18`,\n" +
        "`col19`,\n" +
        "`col20`,\n" +
        "`col21`,\n" +
        "`col22`,\n" +
        "`col23`,\n" +
        "`col24`,\n" +
        "`col25`,\n" +
        "`col26`,\n" +
        "`col27`," +
        ")values(\n" +
        "\t?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?\n" +
        ")";



    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(URL, PORT, DATABASE);
        return JdbcCase.builder().dockerImage(DOCKER_IMAGE).networkAliases(NETWORK_ALIASES).containerEnv(containerEnv).driverClass(DRIVER_CLASS)
            .host(HOST).port(PORT).localPort(PORT).jdbcTemplate(URL).jdbcUrl(jdbcUrl).userName(USERNAME).password(PASSWORD).dataBase(DATABASE)
            .sourceTable(SOURCE_TABLE).driverJar(DRIVER_JAR)
            .ddlSource(DDL_SOURCE).ddlSink(DDL_SINK).initDataSql(INIT_DATA_SQL).configFile(CONFIG_FILE).seaTunnelRow(initTestData()).build();
    }

    @Override
    void compareResult() throws SQLException, IOException {
        try {
            Connection connection = initializeJdbcConnection(URL);
            assertHasData(SOURCE_TABLE);
            assertHasData(SINK_TABLE);
            JdbcCompareUtil.compare(connection,  String.format("select * from %s.%s limit 1", DATABASE, SOURCE_TABLE),
                String.format("select * from %s.%s limit 1", DATABASE, SINK_TABLE), COLUMN_STRING);

        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            e.printStackTrace();
        }


    }

    @Override
    void clearSinkTable() {

    }

    @Override
    SeaTunnelRow initTestData() {
        return new SeaTunnelRow(
            new Object[]{101,2,3,4,5,6,7,8,9.1,10.1,"十一","十二","十三","十四","十五","十六","十七","十八","十九","二十","二十一","二十二","19700101","00:00:00","19700101","19700101","1970"}
        );
    }

    private void assertHasData(String table) {
        try (Statement statement = initializeJdbcConnection(URL).createStatement()) {
            String sql = String.format("select * from %s.%s limit 1", DATABASE, table);
            ResultSet source = statement.executeQuery(sql);
            Assertions.assertTrue(source.next());
        } catch (Exception e) {
            throw new RuntimeException("test oceanbase server image error", e);
        }
    }
}

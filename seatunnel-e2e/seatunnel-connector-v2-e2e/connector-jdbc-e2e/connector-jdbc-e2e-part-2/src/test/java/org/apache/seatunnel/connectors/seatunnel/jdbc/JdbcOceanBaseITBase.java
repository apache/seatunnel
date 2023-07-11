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

import org.junit.jupiter.api.Assertions;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class JdbcOceanBaseITBase extends AbstractJdbcIT {

    private static final String OCEANBASE_DATABASE = "seatunnel";
    private static final String OCEANBASE_SOURCE = "source";
    private static final String OCEANBASE_SINK = "sink";

    private static final String OCEANBASE_JDBC_TEMPLATE = "jdbc:oceanbase://" + HOST + ":%s";
    private static final String OCEANBASE_DRIVER_CLASS = "com.oceanbase.jdbc.Driver";

    abstract String imageName();

    abstract String host();

    abstract int port();

    abstract String username();

    abstract String password();

    abstract List<String> configFile();

    abstract String createSqlTemplate();

    abstract String[] getFieldNames();

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(OCEANBASE_JDBC_TEMPLATE, port());
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(OCEANBASE_DATABASE, OCEANBASE_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(imageName())
                .networkAliases(host())
                .containerEnv(containerEnv)
                .driverClass(OCEANBASE_DRIVER_CLASS)
                .host(HOST)
                .port(port())
                .localPort(port())
                .jdbcTemplate(OCEANBASE_JDBC_TEMPLATE)
                .jdbcUrl(jdbcUrl)
                .userName(username())
                .password(password())
                .database(OCEANBASE_DATABASE)
                .sourceTable(OCEANBASE_SOURCE)
                .sinkTable(OCEANBASE_SINK)
                .createSql(createSqlTemplate())
                .configFile(configFile())
                .insertSql(insertSql)
                .testData(testDataSet)
                .build();
    }

    @Override
    void compareResult() {
        String sourceSql =
                String.format(
                        "select * from %s.%s order by 1", OCEANBASE_DATABASE, OCEANBASE_SOURCE);
        String sinkSql =
                String.format("select * from %s.%s order by 1", OCEANBASE_DATABASE, OCEANBASE_SINK);
        try {
            Statement sourceStatement = connection.createStatement();
            Statement sinkStatement = connection.createStatement();
            ResultSet sourceResultSet = sourceStatement.executeQuery(sourceSql);
            ResultSet sinkResultSet = sinkStatement.executeQuery(sinkSql);
            Assertions.assertEquals(
                    sourceResultSet.getMetaData().getColumnCount(),
                    sinkResultSet.getMetaData().getColumnCount());
            while (sourceResultSet.next()) {
                if (sinkResultSet.next()) {
                    for (String column : getFieldNames()) {
                        Object source = sourceResultSet.getObject(column);
                        Object sink = sinkResultSet.getObject(column);
                        if (!Objects.deepEquals(source, sink)) {
                            InputStream sourceAsciiStream = sourceResultSet.getBinaryStream(column);
                            InputStream sinkAsciiStream = sinkResultSet.getBinaryStream(column);
                            String sourceValue =
                                    IOUtils.toString(sourceAsciiStream, StandardCharsets.UTF_8);
                            String sinkValue =
                                    IOUtils.toString(sinkAsciiStream, StandardCharsets.UTF_8);
                            Assertions.assertEquals(sourceValue, sinkValue);
                        }
                    }
                }
            }
            sourceResultSet.last();
            sinkResultSet.last();
        } catch (Exception e) {
            throw new RuntimeException("Compare result error", e);
        }
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/oceanbase/oceanbase-client/2.4.3/oceanbase-client-2.4.3.jar";
    }

    @Override
    protected void createSchemaIfNeeded() {
        String sql = "CREATE DATABASE IF NOT EXISTS " + OCEANBASE_DATABASE;
        try {
            connection.prepareStatement(sql).executeUpdate();
        } catch (Exception e) {
            throw new SeaTunnelRuntimeException(
                    JdbcITErrorCode.CREATE_TABLE_FAILED, "Fail to execute sql " + sql, e);
        }
    }
}

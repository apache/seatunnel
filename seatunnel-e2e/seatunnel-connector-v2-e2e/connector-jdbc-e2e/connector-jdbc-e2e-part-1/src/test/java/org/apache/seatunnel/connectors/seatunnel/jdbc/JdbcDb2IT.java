/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.testcontainers.containers.Db2Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcDb2IT extends AbstractJdbcIT {

    private static final String DB2_CONTAINER_HOST = "db2-e2e";

    private static final String DB2_DATABASE = "E2E";
    private static final String DB2_SOURCE = "SOURCE";
    private static final String DB2_SINK = "SINK";

    private static final String DB2_URL = "jdbc:db2://" + HOST + ":%s/%s";

    private static final String DRIVER_CLASS = "com.ibm.db2.jcc.DB2Driver";

    private static final List<String> CONFIG_FILE =
            Lists.newArrayList("/jdbc_db2_source_and_sink.conf");

    /** <a href="https://hub.docker.com/r/ibmcom/db2">db2 in dockerhub</a> */
    private static final String DB2_IMAGE = "ibmcom/db2";

    private static final int PORT = 50000;
    private static final int LOCAL_PORT = 50000;
    private static final String DB2_USER = "db2inst1";
    private static final String DB2_PASSWORD = "123456";

    private static final String CREATE_SQL =
            "create table %s\n"
                    + "(\n"
                    + "    C_BOOLEAN          BOOLEAN,\n"
                    + "    C_SMALLINT         SMALLINT,\n"
                    + "    C_INT              INTEGER,\n"
                    + "    C_INTEGER          INTEGER,\n"
                    + "    C_BIGINT           BIGINT,\n"
                    + "    C_DECIMAL          DECIMAL(5),\n"
                    + "    C_DEC              DECIMAL(5),\n"
                    + "    C_NUMERIC          DECIMAL(5),\n"
                    + "    C_NUM              DECIMAL(5),\n"
                    + "    C_REAL             REAL,\n"
                    + "    C_FLOAT            DOUBLE,\n"
                    + "    C_DOUBLE           DOUBLE,\n"
                    + "    C_DOUBLE_PRECISION DOUBLE,\n"
                    + "    C_CHAR             CHARACTER(1),\n"
                    + "    C_VARCHAR          VARCHAR(255),\n"
                    + "    C_BINARY           BINARY(1),\n"
                    + "    C_VARBINARY        VARBINARY(2048),\n"
                    + "    C_DATE             DATE\n"
                    + ");\n";

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        String jdbcUrl = String.format(DB2_URL, PORT, DB2_DATABASE);
        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(DB2_DATABASE, DB2_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(DB2_IMAGE)
                .networkAliases(DB2_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(PORT)
                .localPort(PORT)
                .jdbcTemplate(DB2_URL)
                .jdbcUrl(jdbcUrl)
                .userName(DB2_USER)
                .password(DB2_PASSWORD)
                .database(DB2_DATABASE)
                .sourceTable(DB2_SOURCE)
                .sinkTable(DB2_SINK)
                .createSql(CREATE_SQL)
                .configFile(CONFIG_FILE)
                .insertSql(insertSql)
                .testData(testDataSet)
                .build();
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/ibm/db2/jcc/db2jcc/db2jcc4/db2jcc-db2jcc4.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames = {
            "C_BOOLEAN",
            "C_SMALLINT",
            "C_INT",
            "C_INTEGER",
            "C_BIGINT",
            "C_DECIMAL",
            "C_DEC",
            "C_NUMERIC",
            "C_NUM",
            "C_REAL",
            "C_FLOAT",
            "C_DOUBLE",
            "C_DOUBLE_PRECISION",
            "C_CHAR",
            "C_VARCHAR",
            "C_BINARY",
            "C_VARBINARY",
            "C_DATE",
        };

        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                i % 2 == 0 ? Boolean.TRUE : Boolean.FALSE,
                                Short.valueOf("1"),
                                i,
                                i,
                                Long.parseLong("1"),
                                BigDecimal.valueOf(i, 0),
                                BigDecimal.valueOf(i, 18),
                                BigDecimal.valueOf(i, 18),
                                BigDecimal.valueOf(i, 18),
                                Float.parseFloat("1.1"),
                                Double.parseDouble("1.1"),
                                Double.parseDouble("1.1"),
                                Double.parseDouble("1.1"),
                                "f",
                                String.format("f1_%s", i),
                                "f".getBytes(),
                                "test".getBytes(),
                                Date.valueOf(LocalDate.now()),
                            });
            rows.add(row);
        }
        return Pair.of(fieldNames, rows);
    }

    @Override
    protected GenericContainer<?> initContainer() {
        GenericContainer<?> container =
                new Db2Container(DB2_IMAGE)
                        .withExposedPorts(PORT)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(DB2_CONTAINER_HOST)
                        .withDatabaseName(DB2_DATABASE)
                        .withUsername(DB2_USER)
                        .withPassword(DB2_PASSWORD)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DB2_IMAGE)))
                        .acceptLicense();
        container.setPortBindings(Lists.newArrayList(String.format("%s:%s", LOCAL_PORT, PORT)));

        return container;
    }

    @Override
    public String quoteIdentifier(String field) {
        return "\"" + field + "\"";
    }

    @Override
    public void clearTable(String schema, String table) {
        try (Statement statement = connection.createStatement()) {
            String truncate =
                    String.format(
                            "delete from %s where 1=1;", buildTableInfoWithSchema(schema, table));
            statement.execute(truncate);
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException exception) {
                throw new SeaTunnelRuntimeException(JdbcITErrorCode.CLEAR_TABLE_FAILED, exception);
            }
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.CLEAR_TABLE_FAILED, e);
        }
    }
}

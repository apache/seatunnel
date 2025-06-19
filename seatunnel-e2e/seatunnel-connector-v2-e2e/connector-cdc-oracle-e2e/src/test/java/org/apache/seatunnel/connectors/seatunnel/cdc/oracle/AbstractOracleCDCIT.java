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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle;

import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;
import oracle.sql.TIMESTAMP;
import oracle.sql.TIMESTAMPLTZ;

import java.math.BigDecimal;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertNotNull;

@Slf4j
public class AbstractOracleCDCIT extends TestSuiteBase {

    protected static final String ORACLE_IMAGE = "goodboy008/oracle-19.3.0-ee:non-cdb";

    protected static final String ORACLE_IMAGE_ARM = "goodboy008/oracle-19.3.0-ee:arm-non-cdb";

    protected static final String HOST = "oracle-host";

    protected static final Integer ORACLE_PORT = 1521;

    protected static final String CONNECTOR_USER = "dbzuser";

    protected static final String CONNECTOR_PWD = "dbz";

    protected static final String SCHEMA_USER = "debezium";

    protected static final String SCHEMA_PWD = "dbz";

    public static final String ADMIN_USER = "sys as sysdba";

    public static final String ADMIN_PWD = "top_secret";

    protected static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    protected static final String SCEHMA_NAME = "DEBEZIUM";

    protected static final String SOURCE_TABLE1 = "FULL_TYPES";

    protected static final String SOURCE_TABLE2 = "FULL_TYPES2";

    protected static final OracleContainer ORACLE_CONTAINER =
            new OracleContainer(getImage())
                    .withUsername(CONNECTOR_USER)
                    .withPassword(CONNECTOR_PWD)
                    .withDatabaseName("ORCLCDB")
                    .withNetwork(NETWORK)
                    .withNetworkAliases(HOST)
                    .withExposedPorts(ORACLE_PORT)
                    .withLogConsumer(
                            new Slf4jLogConsumer(
                                    DockerLoggerFactory.getLogger("oracle-docker-image")));

    private static String getImage() {
        // If the current environment is ARM architecture, then use the ARM image
        if (System.getProperty("os.arch").equals("aarch64")) {
            return ORACLE_IMAGE_ARM;
        } else {
            return ORACLE_IMAGE;
        }
    }

    protected String oracleDriverUrl() {
        return "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/12.2.0.1/ojdbc8-12.2.0.1.jar";
    }

    static {
        System.setProperty("oracle.jdbc.timezoneAsRegion", "false");
    }

    protected static void createAndInitialize(String sqlFile, String username, String password)
            throws Exception {
        final String ddlFile = String.format("ddl/%s.sql", sqlFile);
        final URL ddlTestFile = OracleCDCIT.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try (Connection connection =
                        getJdbcConnection(ORACLE_CONTAINER.getJdbcUrl(), username, password);
                Statement statement = connection.createStatement()) {

            final List<String> statements =
                    Arrays.stream(
                                    Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream()
                                            .map(String::trim)
                                            .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                            .map(
                                                    x -> {
                                                        final Matcher m =
                                                                COMMENT_PATTERN.matcher(x);
                                                        return m.matches() ? m.group(1) : x;
                                                    })
                                            .collect(Collectors.joining("\n"))
                                            .split(";"))
                            .collect(Collectors.toList());

            for (String stmt : statements) {
                statement.execute(stmt);
            }
        }
    }

    protected static Connection getJdbcConnection(String jdbcUrl, String username, String password)
            throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    protected List<List<String>> query(
            String jdbcUrl, String sql, String userName, String password) {
        try (Connection connection = getJdbcConnection(jdbcUrl, userName, password)) {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            List<List<String>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                ArrayList<String> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = resultSet.getObject(i);
                    objects.add(formatValue(value, columnName, connection));
                }
                log.debug(String.format("Print Oracle-CDC query, sql: %s, data: %s", sql, objects));
                result.add(objects);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static String formatValue(Object value, String columnName, Connection connection)
            throws SQLException {
        if (value == null) {
            return "";
        }
        if (value instanceof Timestamp
                || value instanceof TIMESTAMP
                || value instanceof TIMESTAMPLTZ) {
            Timestamp timestamp;
            if (value instanceof Timestamp) {
                timestamp = (Timestamp) value;
            } else if (value instanceof TIMESTAMP) {
                timestamp = ((TIMESTAMP) value).timestampValue();
            } else {
                timestamp = ((TIMESTAMPLTZ) value).timestampValue(connection);
            }
            ZonedDateTime zonedDateTime = timestamp.toInstant().atZone(ZoneId.systemDefault());
            if (value instanceof TIMESTAMPLTZ) {
                zonedDateTime = zonedDateTime.withZoneSameInstant(ZoneId.of("UTC"));
            }
            LocalDateTime localDateTime = zonedDateTime.toLocalDateTime();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
            return localDateTime.format(formatter);
        }

        if (value instanceof LocalDateTime) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
            return ((LocalDateTime) value).format(formatter);
        }
        if (columnName.equalsIgnoreCase("VAL_NUMBER_1") && value instanceof BigDecimal) {
            BigDecimal bdValue = (BigDecimal) value;
            if (bdValue.compareTo(BigDecimal.ONE) == 0) {
                return "true";
            } else if (bdValue.compareTo(BigDecimal.ZERO) == 0) {
                return "false";
            }
        }
        if (value instanceof Double || value instanceof Float || value instanceof BigDecimal) {
            BigDecimal bd = new BigDecimal(value.toString()).stripTrailingZeros();
            return bd.toPlainString();
        }
        if (value instanceof Boolean) {
            return value.toString();
        }
        return value.toString();
    }

    protected static void dropTable(String jdbcUrl, String schemaName, String tableName) {
        try (Connection connection = getJdbcConnection(jdbcUrl, CONNECTOR_USER, CONNECTOR_PWD);
                Statement statement = connection.createStatement()) {
            ResultSet resultSet =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM ALL_TABLES WHERE OWNER='%s' AND TABLE_NAME='%s'",
                                    schemaName, tableName));
            if (resultSet.next()) {
                statement.execute(String.format("DROP TABLE %s.%s", schemaName, tableName));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

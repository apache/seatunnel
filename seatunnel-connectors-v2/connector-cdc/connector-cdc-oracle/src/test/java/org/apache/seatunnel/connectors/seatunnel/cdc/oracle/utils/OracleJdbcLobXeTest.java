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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockito.Mockito;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import oracle.jdbc.OracleDriver;
import oracle.jdbc.OracleTypes;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Live JDBC snapshot of CLOB/NCLOB/BLOB/XMLTYPE on the two Oracle XE versions this environment can
 * actually start: 11g ({@code gvenzl/oracle-xe:11-full}) and 21c ({@code
 * gvenzl/oracle-xe:21-slim-faststart}).
 *
 * <p>This is the snapshot converter path, not LogMiner. 11g vs 21c redo dialects are covered by
 * parser unit tests. SeaTunnel CDC E2E still uses the 19c EE image that has archive logs.
 *
 * <p>Run with {@code -Doracle.xe.it=true} so default unit-test CI does not pull Oracle XE.
 */
@EnabledIfSystemProperty(named = "oracle.xe.it", matches = "true")
public class OracleJdbcLobXeTest {

    private static final String ORACLE_11 = "gvenzl/oracle-xe:11-full";
    private static final String ORACLE_21 = "gvenzl/oracle-xe:21-slim-faststart";
    private static final byte[] PNG =
            new byte[] {(byte) 0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A};
    private static final byte[] MP4 =
            new byte[] {0x00, 0x00, 0x00, 0x18, 0x66, 0x74, 0x79, 0x70, 0x69, 0x73, 0x6F, 0x6D};

    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    public void oracle11gSnapshotAndExceptions() throws Exception {
        runAgainst(elevenG());
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    public void oracle21cSnapshotAndExceptions() throws Exception {
        runAgainst(twentyOne());
    }

    private static void runAgainst(OracleContainer oracle) throws Exception {
        Assertions.assertTrue(
                DockerClientFactory.instance().isDockerAvailable(), "Docker is required");
        oracle.start();
        try (Connection connection = open(oracle)) {
            createTable(connection);
            assertSnapshotHappyPath(connection);
            assertEmptyVersusNull(connection);
            assertFreedLocatorFails(connection);
            assertInvalidXmlIsRejected(connection);
            assertMissingTableFails(connection);
        } finally {
            oracle.stop();
        }
    }

    private static void createTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(
                    "BEGIN EXECUTE IMMEDIATE 'DROP TABLE LOB_LIVE'; EXCEPTION WHEN OTHERS THEN"
                            + " IF SQLCODE != -942 THEN RAISE; END IF; END;");
            statement.execute(
                    "CREATE TABLE LOB_LIVE (ID NUMBER PRIMARY KEY, VAL_CLOB CLOB, VAL_NCLOB NCLOB,"
                            + " VAL_BLOB BLOB, VAL_XML XMLTYPE)");
        }
    }

    private static void assertSnapshotHappyPath(Connection connection) throws Exception {
        try (PreparedStatement insert =
                connection.prepareStatement(
                        "INSERT INTO LOB_LIVE (ID, VAL_CLOB, VAL_NCLOB, VAL_BLOB, VAL_XML) VALUES"
                                + " (1, ?, ?, ?, XMLTYPE(?))")) {
            insert.setString(1, "it's a clob \uD83D\uDE00");
            insert.setNString(2, "nclob-emoji \uD83D\uDE00");
            insert.setBytes(3, PNG);
            insert.setString(4, "<note lang=\"zh\">it's xml 中文</note>");
            insert.executeUpdate();
        }
        try (Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO LOB_LIVE (ID, VAL_CLOB, VAL_BLOB) VALUES (2, EMPTY_CLOB(),"
                            + " HEXTORAW('000000186674797069736F6D'))");
        }
        try (PreparedStatement insert =
                connection.prepareStatement("INSERT INTO LOB_LIVE (ID, VAL_CLOB) VALUES (6, ?)")) {
            insert.setString(1, longClob());
            insert.executeUpdate();
        }

        try (Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT VAL_CLOB, VAL_NCLOB, VAL_BLOB, VAL_XML FROM LOB_LIVE WHERE"
                                        + " ID = 1")) {
            Assertions.assertTrue(resultSet.next());
            Clob clob = resultSet.getClob(1);
            Clob nclob = resultSet.getClob(2);
            Blob blob = resultSet.getBlob(3);
            Assertions.assertEquals("it's a clob \uD83D\uDE00", convertClob(clob));
            Assertions.assertEquals("nclob-emoji \uD83D\uDE00", convertClob(nclob));
            Assertions.assertArrayEquals(PNG, convertBlob(blob));
            // XMLType stores a parsed document; apostrophes may appear as &apos;.
            assertXmlContains(readXml(resultSet, 4), "it's xml", "中文");
        }

        try (Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery("SELECT VAL_BLOB FROM LOB_LIVE WHERE ID = 2")) {
            Assertions.assertTrue(resultSet.next());
            Assertions.assertArrayEquals(MP4, convertBlob(resultSet.getBlob(1)));
        }

        try (Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery("SELECT VAL_CLOB FROM LOB_LIVE WHERE ID = 6")) {
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals(longClob(), convertClob(resultSet.getClob(1)));
        }
    }

    private static void assertEmptyVersusNull(Connection connection) throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO LOB_LIVE (ID, VAL_CLOB, VAL_BLOB, VAL_XML) VALUES (3,"
                            + " EMPTY_CLOB(), EMPTY_BLOB(), NULL)");
            statement.execute("INSERT INTO LOB_LIVE (ID, VAL_CLOB) VALUES (5, NULL)");
        }
        try (Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT VAL_CLOB, VAL_BLOB, VAL_XML FROM LOB_LIVE WHERE ID = 3")) {
            Assertions.assertTrue(resultSet.next());
            Assertions.assertEquals("", convertClob(resultSet.getClob(1)));
            Assertions.assertArrayEquals(new byte[0], convertBlob(resultSet.getBlob(2)));
            Assertions.assertNull(resultSet.getObject(3));
        }
        try (Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery("SELECT VAL_CLOB FROM LOB_LIVE WHERE ID = 5")) {
            Assertions.assertTrue(resultSet.next());
            Assertions.assertNull(resultSet.getClob(1));
            Assertions.assertNull(convertClob(null));
        }
    }

    private static void assertFreedLocatorFails(Connection connection) throws Exception {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery("SELECT VAL_CLOB FROM LOB_LIVE WHERE ID = 1")) {
            Assertions.assertTrue(resultSet.next());
            Clob clob = resultSet.getClob(1);
            clob.free();
            Assertions.assertThrows(DebeziumException.class, () -> convertClob(clob));
        }
    }

    private static void assertInvalidXmlIsRejected(Connection connection) {
        SQLException error =
                Assertions.assertThrows(
                        SQLException.class,
                        () -> {
                            try (Statement statement = connection.createStatement()) {
                                statement.execute(
                                        "INSERT INTO LOB_LIVE (ID, VAL_XML) VALUES (4,"
                                                + " XMLTYPE('<not-closed'))");
                            }
                        });
        String message = error.getMessage() == null ? "" : error.getMessage().toUpperCase();
        Assertions.assertTrue(
                message.contains("XML") || message.contains("ORA-"),
                "unexpected XMLTYPE error: " + error.getMessage());
    }

    private static void assertMissingTableFails(Connection connection) {
        Assertions.assertThrows(
                SQLException.class,
                () -> {
                    try (Statement statement = connection.createStatement()) {
                        statement.executeQuery("SELECT VAL_CLOB FROM NO_SUCH_LOB_TABLE");
                    }
                });
    }

    private static String readXml(ResultSet resultSet, int index) throws Exception {
        try {
            SQLXML xml = resultSet.getSQLXML(index);
            if (xml != null) {
                return convertXml(xml);
            }
        } catch (SQLException ignored) {
            // 11g ojdbc8 may not expose XMLTYPE as SQLXML; serialize for the assertion.
        }
        try (Statement statement = resultSet.getStatement().getConnection().createStatement();
                ResultSet serialized =
                        statement.executeQuery(
                                "SELECT XMLSERIALIZE(CONTENT VAL_XML AS CLOB NO INDENT) FROM"
                                        + " LOB_LIVE WHERE ID = 1")) {
            Assertions.assertTrue(serialized.next());
            return convertClob(serialized.getClob(1));
        }
    }

    private static void assertXmlContains(String xml, String... fragments) {
        Assertions.assertNotNull(xml);
        String normalized = xml.replace("&apos;", "'").replace("&quot;", "\"");
        for (String fragment : fragments) {
            Assertions.assertTrue(
                    normalized.contains(fragment),
                    "XML snapshot missing '" + fragment + "': " + xml);
        }
    }

    private static String longClob() {
        StringBuilder builder = new StringBuilder(4200);
        while (builder.length() < 4096) {
            builder.append("0123456789abcdef");
        }
        builder.append(" \uD83D\uDE00");
        return builder.toString();
    }

    private static String convertClob(Clob clob) {
        return (String) clobConverter().convert(clob);
    }

    private static String convertXml(SQLXML xml) {
        return (String) xmlConverter().convert(xml);
    }

    private static byte[] convertBlob(Blob blob) {
        Object converted = blobConverter().convert(blob);
        if (converted instanceof byte[]) {
            return (byte[]) converted;
        }
        java.nio.ByteBuffer buffer = (java.nio.ByteBuffer) converted;
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private static ValueConverter clobConverter() {
        return converter(
                Column.editor().name("VAL_CLOB").type("CLOB").jdbcType(Types.CLOB).create(),
                Schema.OPTIONAL_STRING_SCHEMA);
    }

    private static ValueConverter xmlConverter() {
        return converter(
                Column.editor()
                        .name("VAL_XML")
                        .type("XMLTYPE")
                        .jdbcType(OracleTypes.SQLXML)
                        .create(),
                Schema.OPTIONAL_STRING_SCHEMA);
    }

    private static ValueConverter blobConverter() {
        return converter(
                Column.editor().name("VAL_BLOB").type("BLOB").jdbcType(Types.BLOB).create(),
                Schema.OPTIONAL_BYTES_SCHEMA);
    }

    private static ValueConverter converter(Column column, Schema fieldSchema) {
        OracleConnectorConfig config =
                new OracleConnectorConfig(
                        Configuration.create()
                                .with(OracleConnectorConfig.SERVER_NAME, "test_server")
                                .with(OracleConnectorConfig.HOSTNAME, "localhost")
                                .with(OracleConnectorConfig.USER, "test")
                                .with(OracleConnectorConfig.PASSWORD, "test")
                                .with(OracleConnectorConfig.LOB_ENABLED, false)
                                .build());
        OracleJdbcLobValueConverters converters =
                new OracleJdbcLobValueConverters(config, Mockito.mock(OracleConnection.class));
        Schema schema = SchemaBuilder.struct().field(column.name(), fieldSchema).build();
        Field field = schema.field(column.name());
        return converters.converter(column, field);
    }

    private static Connection open(OracleContainer oracle) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", oracle.getUsername());
        properties.setProperty("password", oracle.getPassword());
        // 11g timezone files reject many JVM region names (ORA-01882). Send an offset instead.
        System.setProperty("oracle.jdbc.timezoneAsRegion", "false");
        properties.setProperty("oracle.jdbc.timezoneAsRegion", "false");
        Connection connection = new OracleDriver().connect(oracle.getJdbcUrl(), properties);
        if (connection == null) {
            throw new SQLException("OracleDriver rejected JDBC URL " + oracle.getJdbcUrl());
        }
        return connection;
    }

    private static OracleContainer elevenG() {
        return new OracleContainer(
                        DockerImageName.parse(ORACLE_11)
                                .asCompatibleSubstituteFor("gvenzl/oracle-xe"))
                .withPassword("testpass")
                .usingSid()
                .withEnv("TZ", "UTC")
                .withStartupTimeout(Duration.ofMinutes(5));
    }

    private static OracleContainer twentyOne() {
        return new OracleContainer(DockerImageName.parse(ORACLE_21))
                .withUsername("testuser")
                .withPassword("testpass")
                .withEnv("TZ", "UTC")
                .withStartupTimeout(Duration.ofMinutes(5));
    }
}

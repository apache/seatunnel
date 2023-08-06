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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils;

import org.junit.jupiter.api.Assertions;

import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Create and populate a unique instance of a MySQL database for each run of JUnit test. A user of
 * class needs to provide a logical name for Debezium and database name. It is expected that there
 * is an init file in <code>src/test/resources/ddl/&lt;database_name&gt;.sql</code>. The database
 * name is enriched with a unique suffix that guarantees complete isolation between runs <code>
 * &lt;database_name&gt_&lt;suffix&gt</code>
 *
 * <p>This class is inspired from Debezium project.
 */
@SuppressWarnings("MagicNumber")
@Slf4j
public class UniqueDatabase {

    private static final String[] CREATE_DATABASE_DDL =
            new String[] {"CREATE DATABASE IF NOT EXISTS $DBNAME$;", "USE $DBNAME$;"};
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    private final MySqlContainer container;
    private final String databaseName;
    private final String templateName;
    private final String username;
    private final String password;

    public UniqueDatabase(
            MySqlContainer container, String databaseName, String username, String password) {
        this(
                container,
                databaseName,
                Integer.toUnsignedString(new Random().nextInt(), 36),
                username,
                password);
    }

    private UniqueDatabase(
            MySqlContainer container,
            String databaseName,
            final String identifier,
            String username,
            String password) {
        this.container = container;
        this.databaseName = databaseName + "_" + identifier;
        this.templateName = databaseName;
        this.username = username;
        this.password = password;
    }

    public UniqueDatabase(MySqlContainer container, String databaseName) {
        this.container = container;
        this.databaseName = databaseName;
        this.templateName = databaseName;
        this.username = container.getUsername();
        this.password = container.getPassword();
    }

    public String getHost() {
        return container.getHost();
    }

    public int getDatabasePort() {
        return container.getDatabasePort();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    /** @return Fully qualified table name <code>&lt;databaseName&gt;.&lt;tableName&gt;</code> */
    public String qualifiedTableName(final String tableName) {
        return String.format("%s.%s", databaseName, tableName);
    }

    /** Creates the database and populates it with initialization SQL script. */
    public void createAndInitialize() {
        final String ddlFile = String.format("ddl/%s.sql", templateName);
        final URL ddlTestFile = UniqueDatabase.class.getClassLoader().getResource(ddlFile);
        Assertions.assertNotNull(ddlTestFile, "Cannot locate " + ddlFile);
        try {
            try (Connection connection =
                            DriverManager.getConnection(
                                    container.getJdbcUrl(), username, password);
                    Statement statement = connection.createStatement()) {
                final List<String> statements =
                        Arrays.stream(
                                        Stream.concat(
                                                        Arrays.stream(CREATE_DATABASE_DDL),
                                                        Files.readAllLines(
                                                                Paths.get(ddlTestFile.toURI()))
                                                                .stream())
                                                .map(String::trim)
                                                .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                                .map(
                                                        x -> {
                                                            final Matcher m =
                                                                    COMMENT_PATTERN.matcher(x);
                                                            return m.matches() ? m.group(1) : x;
                                                        })
                                                .map(this::convertSQL)
                                                .collect(Collectors.joining("\n"))
                                                .split(";"))
                                .map(x -> x.replace("$$", ";"))
                                .collect(Collectors.toList());
                for (String stmt : statements) {
                    statement.execute(stmt);
                    log.info(stmt);
                }
            }
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(container.getJdbcUrl(databaseName), username, password);
    }

    private String convertSQL(final String sql) {
        return sql.replace("$DBNAME$", databaseName);
    }
}

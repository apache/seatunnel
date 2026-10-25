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

package org.apache.seatunnel.e2e.connector.edgesocket;

import org.junit.jupiter.api.Assertions;

import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Create and initialize an isolated MySQL database from ddl templates. */
@Slf4j
class EdgeSocketUniqueDatabase {

    private static final String[] CREATE_DATABASE_DDL =
            new String[] {
                "CREATE DATABASE IF NOT EXISTS $DBNAME$ CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;",
                "USE $DBNAME$;"
            };
    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");
    private final EdgeSocketMySqlContainer container;
    private final String databaseName;
    private final String username;
    private final String password;
    private String templateName;

    EdgeSocketUniqueDatabase(
            EdgeSocketMySqlContainer container,
            String databaseName,
            String username,
            String password,
            String templateName) {
        this.container = container;
        this.databaseName = databaseName;
        this.username = username;
        this.password = password;
        this.templateName = templateName;
    }

    EdgeSocketUniqueDatabase setTemplateName(String templateName) {
        this.templateName = templateName;
        return this;
    }

    String getDatabaseName() {
        return databaseName;
    }

    Connection getJdbcConnection() throws Exception {
        return DriverManager.getConnection(getJdbcUrl(), username, password);
    }

    /** Creates the database and initializes schema/data using ddl template. */
    void createAndInitialize() {
        final String ddlFile = String.format("ddl/%s.sql", templateName);
        final URL ddlTestFile =
                EdgeSocketUniqueDatabase.class.getClassLoader().getResource(ddlFile);
        Assertions.assertNotNull(ddlTestFile, "Cannot locate " + ddlFile);
        try (Connection connection =
                        DriverManager.getConnection(getServerJdbcUrl(), username, password);
                Statement statement = connection.createStatement()) {
            List<String> statements =
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
                                                        Matcher matcher =
                                                                COMMENT_PATTERN.matcher(x);
                                                        return matcher.matches()
                                                                ? matcher.group(1)
                                                                : x;
                                                    })
                                            .map(this::convertSql)
                                            .collect(Collectors.joining("\n"))
                                            .split(";"))
                            .map(x -> x.replace("$$", ";"))
                            .map(String::trim)
                            .filter(x -> !x.isEmpty())
                            .collect(Collectors.toList());
            for (String stmt : statements) {
                statement.execute(stmt);
                log.info(stmt);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private String convertSql(String sql) {
        return sql.replace("$DBNAME$", databaseName);
    }

    private String getJdbcUrl() {
        return String.format(
                "jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true",
                container.getHost(), container.getDatabasePort(), databaseName);
    }

    private String getServerJdbcUrl() {
        return String.format(
                "jdbc:mysql://%s:%d/?useSSL=false&allowPublicKeyRetrieval=true",
                container.getHost(), container.getDatabasePort());
    }
}

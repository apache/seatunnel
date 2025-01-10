/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.commons.lang3.tuple.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.utility.DockerImageName;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class JdbcWildcardMysqlIT extends AbstractJdbcWildcardIT {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcWildcardMysqlIT.class);
    private static final String DATABASE_TYPE = "mysql";
    private static final String MYSQL_IMAGE = "mysql:8.0";
    private static final String MYSQL_CONTAINER_HOST = "mysql_e2e";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "Abc!@#135_seatunnel";
    private static final int MYSQL_PORT = 3306;
    private static final String SOURCE_DATABASE = "source";
    private static final String SINK_DATABASE = "sink";
    private static final String MYSQL_URL = "jdbc:mysql://" + HOST + ":%s/%s?useSSL=false";
    private static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    private static final String CREATE_DATABASE_TEMPLATE = "CREATE DATABASE IF NOT EXISTS %s";
    private static final String CREATE_TABLE_TEMPLATE =
            "CREATE TABLE IF NOT EXISTS %s (`id` INT NOT NULL, `name` VARCHAR(255), `desc` VARCHAR(255), PRIMARY KEY (`id`))";

    @Override
    JdbcWildcardCase getJdbcWildcardsCase() {
        Pair<String[], List<SeaTunnelRow>> testData = initTestData();
        String columns =
                Arrays.stream(testData.getLeft())
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(testData.getLeft()).map(f -> "?").collect(Collectors.joining(", "));
        String INSERT_DATA_TEMPLATE =
                "INSERT INTO %s (" + columns + ") VALUES (" + placeholders + ")";
        return JdbcWildcardCase.builder()
                .databaseType(DATABASE_TYPE)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .userName(MYSQL_USERNAME)
                .password(MYSQL_PASSWORD)
                .port(MYSQL_PORT)
                .jdbcUrl(String.format(MYSQL_URL, MYSQL_PORT, SOURCE_DATABASE))
                .configFile("/jdbc_wildcards_mysql_source_to_sink.conf")
                .sourceDatabase(SOURCE_DATABASE)
                .sinkDatabase(SINK_DATABASE)
                .createDatabaseTemplate(CREATE_DATABASE_TEMPLATE)
                .createTableTemplate(CREATE_TABLE_TEMPLATE)
                .insertDataTableTemplate(INSERT_DATA_TEMPLATE)
                .sourceTable(Lists.newArrayList("test1", "test2"))
                .sinkTable(Lists.newArrayList("source_test1", "source_test2"))
                .testData(testData)
                .build();
    }

    @Override
    String driverUrl() {
        return "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";
    }

    @Override
    GenericContainer<?> initContainer() {
        DockerImageName imageName = DockerImageName.parse(MYSQL_IMAGE);

        GenericContainer<?> container =
                new MySQLContainer<>(imageName)
                        .withUsername(MYSQL_USERNAME)
                        .withPassword(MYSQL_PASSWORD)
                        .withDatabaseName(SOURCE_DATABASE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_CONTAINER_HOST)
                        .withExposedPorts(MYSQL_PORT)
                        .withImagePullPolicy(PullPolicy.defaultPolicy())
                        .waitingFor(Wait.forHealthcheck())
                        .withLogConsumer(new Slf4jLogConsumer(LOG));

        container.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", MYSQL_PORT, MYSQL_PORT)));

        return container;
    }
}

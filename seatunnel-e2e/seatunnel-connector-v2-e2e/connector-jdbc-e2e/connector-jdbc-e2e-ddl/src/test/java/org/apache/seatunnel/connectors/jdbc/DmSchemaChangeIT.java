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

package org.apache.seatunnel.connectors.jdbc;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

public class DmSchemaChangeIT extends AbstractSchemaChangeBaseIT {

    private static final String DATABASE_TYPE = "Dameng";
    private static final String DM_IMAGE = "laglangyue/dmdb8";
    private static final String DM_CONTAINER_HOST = "e2e_dmdb";
    private static final String DM_DATABASE = "SYSDBA";
    private static final String DM_USERNAME = "SYSDBA";
    private static final String DM_PASSWORD = "SYSDBA";
    private static final int DM_PORT = 5236;
    private static final String DM_URL = "jdbc:dm://%s:%s/%s";

    private static final String DRIVER_CLASS = "dm.jdbc.driver.DmDriver";

    private static final String DM_DRIVER_JAR =
            "https://repo1.maven.org/maven2/com/dameng/DmJdbcDriver18/8.1.1.193/DmJdbcDriver18-8.1.1.193.jar";
    private final String schemaEvolutionCase_config = "/mysqlcdc_to_dm_with_schema_change.conf";
    private final String schemaEvolutionCaseExactlyOnce_config =
            "/mysqlcdc_to_dm_with_schema_change_exactly_once.conf";
    private final String QUERRY_COLUMNS =
            "SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE OWNER = '%s' AND TABLE_NAME = '%s' ORDER by COLUMN_NAME";

    @Override
    protected SchemaChangeCase getSchemaChangeCase() {
        return SchemaChangeCase.builder()
                .jdbcUrl(DM_URL)
                .username(DM_USERNAME)
                .password(DM_PASSWORD)
                .driverUrl(DM_DRIVER_JAR)
                .port(DM_PORT)
                .driverClassName(DRIVER_CLASS)
                .databaseName(DM_DATABASE)
                .schemaName(DM_USERNAME)
                .schemaEvolutionCase(schemaEvolutionCase_config)
                .sinkTable1(SINK_TABLE1)
                .openExactlyOnce(true)
                .schemaEvolutionCaseExactlyOnce(schemaEvolutionCaseExactlyOnce_config)
                .sinkTable2(SINK_TABLE2)
                .sinkQueryColumns(QUERRY_COLUMNS)
                .build();
    }

    @Override
    protected GenericContainer initSinkContainer() {
        GenericContainer<?> container =
                new GenericContainer<>(DM_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(DM_CONTAINER_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DM_IMAGE)));
        container.setPortBindings(Lists.newArrayList(String.format("%s:%s", DM_PORT, DM_PORT)));
        container.setPrivilegedMode(true);
        return container;
    }

    @Override
    protected String sinkDatabaseType() {
        return DATABASE_TYPE;
    }
}

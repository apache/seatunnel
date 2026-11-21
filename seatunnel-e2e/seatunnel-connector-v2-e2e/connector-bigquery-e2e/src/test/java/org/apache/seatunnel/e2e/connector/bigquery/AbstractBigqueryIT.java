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

package org.apache.seatunnel.e2e.connector.bigquery;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.BigQueryEmulatorContainer;
import org.testcontainers.lifecycle.Startables;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.stream.Stream;

@Slf4j
public abstract class AbstractBigqueryIT extends TestSuiteBase implements TestResource {

    private static final String DOCKER_IMAGE = "ghcr.io/goccy/bigquery-emulator:0.8.1";
    protected static final String EMULATOR_NETWORK_ALIAS = "bigquery-emulator";
    protected static final int EMULATOR_REST_PORT = 9050;
    protected static final int EMULATOR_GRPC_PORT = 9060;
    public static final String DATASET_NAME = "test_dataset";
    public static final String TABLE_NAME = "test_table";
    public static final String PROJECT_NAME = "test-project";

    protected BigQueryEmulatorContainer container;
    protected BigQuery bigquery;
    protected TableId tableId;

    @BeforeEach
    @Override
    public void startUp() {
        container =
                new BigQueryEmulatorContainer(DOCKER_IMAGE)
                        .withExposedPorts(EMULATOR_REST_PORT, EMULATOR_GRPC_PORT)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(EMULATOR_NETWORK_ALIAS);

        Startables.deepStart(Stream.of(container)).join();
        log.info("BigQuery emulator container started");

        initialize();
        log.info("BigQuery emulator initialized");
    }

    private void initialize() {
        this.bigquery =
                BigQueryOptions.newBuilder()
                        .setHost(container.getEmulatorHttpEndpoint())
                        .setProjectId(PROJECT_NAME)
                        .setCredentials(NoCredentials.getInstance())
                        .build()
                        .getService();

        bigquery.create(DatasetInfo.newBuilder(DATASET_NAME).build());

        this.tableId = TableId.of(PROJECT_NAME, DATASET_NAME, TABLE_NAME);

        bigquery.create(
                TableInfo.newBuilder(
                                tableId,
                                StandardTableDefinition.of(
                                        Schema.of(
                                                Field.newBuilder(
                                                                "c_map",
                                                                StandardSQLTypeName.STRUCT,
                                                                Field.of(
                                                                        "key",
                                                                        StandardSQLTypeName.STRING),
                                                                Field.of(
                                                                        "value",
                                                                        StandardSQLTypeName.STRING))
                                                        .build(),
                                                Field.newBuilder(
                                                                "c_array",
                                                                StandardSQLTypeName.INT64)
                                                        .setMode(Field.Mode.REPEATED)
                                                        .build(),
                                                Field.of("c_string", StandardSQLTypeName.STRING),
                                                Field.of("c_boolean", StandardSQLTypeName.BOOL),
                                                Field.of("c_tinyint", StandardSQLTypeName.INT64),
                                                Field.of("c_smallint", StandardSQLTypeName.INT64),
                                                Field.of("c_int", StandardSQLTypeName.INT64),
                                                Field.of("c_bigint", StandardSQLTypeName.INT64),
                                                Field.of("c_float", StandardSQLTypeName.FLOAT64),
                                                Field.of("c_double", StandardSQLTypeName.FLOAT64),
                                                Field.of("c_decimal", StandardSQLTypeName.NUMERIC),
                                                Field.of("c_bytes", StandardSQLTypeName.BYTES),
                                                Field.of("c_date", StandardSQLTypeName.DATE),
                                                Field.of(
                                                        "c_timestamp",
                                                        StandardSQLTypeName.TIMESTAMP),
                                                Field.of("c_time", StandardSQLTypeName.TIME))))
                        .build());
    }

    @AfterEach
    @Override
    public void tearDown() {
        if (container != null) {
            container.close();
            container = null;
        }
        bigquery = null;
        tableId = null;
    }
}

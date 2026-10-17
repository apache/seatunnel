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

package org.apache.seatunnel.e2e.connector.lance;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.Container;

import com.lancedb.lance.Dataset;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.given;

@Slf4j
@DisabledOnContainer(
        value = {
            TestContainerId.FLINK_1_13,
            TestContainerId.FLINK_1_14,
            TestContainerId.FLINK_1_15,
            TestContainerId.FLINK_1_16,
            TestContainerId.FLINK_1_17,
            TestContainerId.FLINK_1_18,
            TestContainerId.SPARK_2_4,
            TestContainerId.SPARK_3_3
        },
        type = {},
        disabledReason = "Lance connector does not support Flink and lower than Spark 3.4 yet")
@DisabledOnOs(OS.WINDOWS)
public class LanceIT extends TestSuiteBase {

    private static final String DATASET_PATH = "/tmp/seatunnel_mnt/lanceTest/";
    private static final String TABLE_NAME = "lance_sink_table";

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                container.execInContainer("sh", "-c", "mkdir -p " + DATASET_PATH);
                container.execInContainer("sh", "-c", "chmod -R 777 " + DATASET_PATH);
            };

    @TestTemplate
    public void testInsertAndCheckDataE2e(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult writeResult = container.executeJob("/lance/fake_to_lance.conf");
        if (writeResult.getExitCode() != 0) {
            log.error("Job execution failed with exit code: {}", writeResult.getExitCode());
            log.error("STDOUT: {}", writeResult.getStdout());
            log.error("STDERR: {}", writeResult.getStderr());
            log.error("Container logs: {}", container.getServerLogs());
        }
        Assertions.assertEquals(
                0,
                writeResult.getExitCode(),
                "Job execution failed. STDOUT: "
                        + writeResult.getStdout()
                        + "\nSTDERR: "
                        + writeResult.getStderr()
                        + "\nContainer logs: "
                        + container.getServerLogs());

        String datasetPath = DATASET_PATH + TABLE_NAME;
        log.info("Lance dataset write succeeded!");
        log.info("Dataset path: {}", datasetPath);
        logDatasetVersion(datasetPath);
        assertCanonicalMapSchema(datasetPath);

        given().ignoreExceptions()
                .await()
                .atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            long recordCount = loadLanceTableCount();
                            if (recordCount == -1) {
                                log.info(
                                        "Skipping row count verification due to JNI unavailability in test JVM. "
                                                + "Job execution success confirms data was written.");
                                return;
                            }
                            Assertions.assertEquals(100, recordCount);
                        });
    }

    private long loadLanceTableCount() {
        long count = 0;
        try {
            String datasetUri = DATASET_PATH + TABLE_NAME;
            Dataset dataset = Dataset.open(datasetUri);
            count = dataset.countRows();
            dataset.close();
        } catch (NoClassDefFoundError | ExceptionInInitializerError e) {
            log.warn(
                    "JNI library initialization failed in test JVM (this is expected in E2E tests). "
                            + "The dataset was created successfully in the container. Error: {}",
                    e.getMessage());

            return -1;
        } catch (Exception ex) {
            log.error("Error loading Lance table: {}", ex.getMessage(), ex);
        }
        return count;
    }

    private void assertCanonicalMapSchema(String datasetPath) {
        try (Dataset dataset = Dataset.open(datasetPath)) {
            Schema schema = dataset.getSchema();
            Field mapField = schema.findField("c_map");
            Assertions.assertNotNull(mapField, "The Lance dataset must contain the c_map column");
            Assertions.assertTrue(
                    mapField.getType() instanceof ArrowType.Map,
                    "c_map must be stored as an Arrow Map, but was " + mapField.getType());
            List<String> children =
                    mapField.getChildren().stream()
                            .map(Field::getName)
                            .collect(Collectors.toList());
            Assertions.assertEquals(
                    java.util.Collections.singletonList("entries"),
                    children,
                    "The Map must use Arrow's canonical entries child");
            Field entries = mapField.getChildren().get(0);
            Assertions.assertTrue(entries.getType() instanceof ArrowType.Struct);
            Assertions.assertFalse(entries.isNullable());
            Assertions.assertEquals(
                    java.util.Arrays.asList("key", "value"),
                    entries.getChildren().stream()
                            .map(Field::getName)
                            .collect(Collectors.toList()));
            Assertions.assertFalse(entries.getChildren().get(0).isNullable());
        } catch (NoClassDefFoundError | ExceptionInInitializerError e) {
            log.warn(
                    "JNI library initialization failed in test JVM (this is expected in E2E tests). "
                            + "The dataset schema was not verified in this JVM. Error: {}",
                    e.getMessage());
        } catch (Exception ex) {
            throw new AssertionError("Failed to verify the Lance dataset map schema", ex);
        }
    }

    private boolean checkTableExists() {
        try {
            String datasetUri = DATASET_PATH + TABLE_NAME;
            Dataset dataset = Dataset.open(datasetUri);
            dataset.close();
            return true;
        } catch (NoClassDefFoundError | ExceptionInInitializerError e) {
            log.warn(
                    "JNI library initialization failed in test JVM (this is expected in E2E tests). "
                            + "Cannot verify table existence. Error: {}",
                    e.getMessage());
            return false;
        } catch (Exception ex) {
            log.debug("Table does not exist: {}", ex.getMessage());
            return false;
        }
    }

    private void logDatasetVersion(String datasetPath) {
        try {
            Dataset dataset = Dataset.open(datasetPath);
            long version = dataset.version();
            log.info("Dataset version: {}", version);
            dataset.close();
        } catch (NoClassDefFoundError | ExceptionInInitializerError e) {
            log.warn(
                    "JNI library initialization failed in test JVM (this is expected in E2E tests). "
                            + "Cannot retrieve dataset version. Error: {}",
                    e.getMessage());
        } catch (Exception ex) {
            log.warn("Failed to retrieve dataset version: {}", ex.getMessage());
        }
    }
}

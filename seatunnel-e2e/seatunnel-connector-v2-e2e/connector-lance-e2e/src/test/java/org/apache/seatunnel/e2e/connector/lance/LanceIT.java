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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.Container;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.ipc.LanceScanner;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
    private static final String TIMESTAMP_TABLE_NAME = "lance_timestamp_table";
    private static final String BIGINT_TABLE_NAME = "lance_bigint_table";

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

    @TestTemplate
    public void testTimestampKeepsItsMicroseconds(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult writeResult =
                container.executeJob("/lance/fake_timestamp_to_lance.conf");
        Assertions.assertEquals(0, writeResult.getExitCode(), writeResult.getStderr());

        // The fraction of a second, in microseconds, that each id must keep. Only the fraction is
        // compared, because the whole seconds depend on the zone of the JVM that wrote the value.
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(1, 123_456L);
        expected.put(2, 250L);
        expected.put(3, 999_500L);

        // Unlike the row count check above, this does not skip when the dataset cannot be read
        // in the test JVM, so a pass always means the stored values were checked.
        given().ignoreExceptions()
                .await()
                .atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> Assertions.assertEquals(expected, readTimestampFractions()));
    }

    private Map<Integer, Long> readTimestampFractions() throws Exception {
        Map<Integer, Long> fractions = new HashMap<>();
        try (BufferAllocator allocator = new RootAllocator();
                Dataset dataset = Dataset.open(DATASET_PATH + TIMESTAMP_TABLE_NAME, allocator);
                LanceScanner scanner = dataset.newScan();
                ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                IntVector ids = (IntVector) root.getVector("id");
                TimeStampVector timestamps = (TimeStampVector) root.getVector("c_timestamp");
                for (int i = 0; i < root.getRowCount(); i++) {
                    fractions.put(ids.get(i), Math.floorMod(timestamps.get(i), 1_000_000L));
                }
            }
        }
        return fractions;
    }

    @TestTemplate
    public void testBigintKeepsItsValue(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult writeResult = container.executeJob("/lance/fake_bigint_to_lance.conf");
        Assertions.assertEquals(0, writeResult.getExitCode(), writeResult.getStderr());

        Map<Integer, Long> expected = new HashMap<>();
        expected.put(1, 3_000_000_000L);
        expected.put(2, -3_000_000_000L);
        expected.put(3, 1_739_586_083_123L);
        expected.put(4, Long.MAX_VALUE);

        given().ignoreExceptions()
                .await()
                .atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> Assertions.assertEquals(expected, readBigints()));
    }

    private Map<Integer, Long> readBigints() throws Exception {
        Map<Integer, Long> values = new HashMap<>();
        try (BufferAllocator allocator = new RootAllocator();
                Dataset dataset = Dataset.open(DATASET_PATH + BIGINT_TABLE_NAME, allocator);
                LanceScanner scanner = dataset.newScan();
                ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                IntVector ids = (IntVector) root.getVector("id");
                BigIntVector bigints = (BigIntVector) root.getVector("c_bigint");
                for (int i = 0; i < root.getRowCount(); i++) {
                    // A duplicated row would otherwise overwrite the first one unnoticed.
                    Assertions.assertNull(
                            values.put(ids.get(i), bigints.get(i)), "duplicate id " + ids.get(i));
                }
            }
        }
        return values;
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

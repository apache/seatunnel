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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.Container;

import com.lancedb.lance.Dataset;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.given;

@Slf4j
@DisabledOnContainer(
        value = {TestContainerId.SPARK_2_4},
        type = {},
        disabledReason = "Lance connector does not support Spark 2.4")
@DisabledOnOs(OS.WINDOWS)
public class LanceIT extends TestSuiteBase {

    private static final String DATASET_PATH = "/tmp/seatunnel_mnt/lance/";
    private static final String TABLE_NAME = "lance_sink_table";

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                // Manually create lance dataset directory in container
                container.execInContainer("sh", "-c", "mkdir -p " + DATASET_PATH);
                container.execInContainer("sh", "-c", "chmod -R 777 " + DATASET_PATH);
            };

    @TestTemplate
    public void testInsertAndCheckDataE2e(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult writeResult = container.executeJob("/lance/fake_to_lance.conf");
        Assertions.assertEquals(0, writeResult.getExitCode());

        // Wait for data to be written and verify
        given().ignoreExceptions()
                .await()
                .atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            long recordCount = loadLanceTableCount();
                            Assertions.assertEquals(100, recordCount);
                        });
    }

    private long loadLanceTableCount() {
        long count = 0;
        try {
            // Directly open the dataset and count rows
            String datasetUri = DATASET_PATH + TABLE_NAME;
            Dataset dataset = Dataset.open(datasetUri);
            count = dataset.countRows();
            dataset.close();
        } catch (Exception ex) {
            log.error("Error loading Lance table: {}", ex.getMessage());
        }
        return count;
    }

    private boolean checkTableExists() {
        try {
            String datasetUri = DATASET_PATH + TABLE_NAME;
            Dataset dataset = Dataset.open(datasetUri);
            dataset.close();
            return true;
        } catch (Exception ex) {
            log.debug("Table does not exist: {}", ex.getMessage());
            return false;
        }
    }
}

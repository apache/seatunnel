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

import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
public class BigqueryIT extends AbstractBigqueryIT {
    private static final String STREAMING_TABLE_NAME = "streaming_test_table";

    @TestTemplate
    void testBigQuerySink(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/fake_to_bigquery_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        TableResult result =
                bigquery.query(
                        QueryJobConfiguration.of(
                                String.format(
                                        "SELECT COUNT(*) FROM `%s.%s.%s`",
                                        PROJECT_NAME, DATASET_NAME, TABLE_NAME)));
        Assertions.assertEquals(10L, result.iterateAll().iterator().next().get(0).getLongValue());
    }

    @TestTemplate
    void testBigQueryStreamingSink(TestContainer container)
            throws IOException, InterruptedException {
        createStreamingTable();

        Container.ExecResult execResult =
                container.executeJob("/fake_to_bigquery_streaming_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        Set<List<Object>> expected =
                Stream.<List<Object>>of(
                                Arrays.asList(1L, "Alice"),
                                Arrays.asList(2L, "Bob"),
                                Arrays.asList(3L, "Carol"))
                        .collect(Collectors.toSet());

        TableResult result =
                bigquery.query(
                        QueryJobConfiguration.of(
                                String.format(
                                        "SELECT id, name FROM `%s.%s.%s`",
                                        PROJECT_NAME, DATASET_NAME, STREAMING_TABLE_NAME)));
        Set<List<Object>> actual =
                StreamSupport.stream(result.iterateAll().spliterator(), false)
                        .map(
                                row ->
                                        Arrays.<Object>asList(
                                                row.get(0).getLongValue(),
                                                row.get(1).getStringValue()))
                        .collect(Collectors.toSet());
        Assertions.assertEquals(expected, actual);
    }

    private void createStreamingTable() throws InterruptedException {
        bigquery.query(
                QueryJobConfiguration.of(
                        String.format(
                                "CREATE TABLE `%s.%s.%s` ("
                                        + "id INT64 NOT NULL, "
                                        + "name STRING, "
                                        + "PRIMARY KEY (id) NOT ENFORCED"
                                        + ")",
                                PROJECT_NAME, DATASET_NAME, STREAMING_TABLE_NAME)));
    }
}

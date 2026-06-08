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

package org.apache.seatunnel.connectors.bigquery.sink.writer;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.bigquery.client.BigQueryClientFactory;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorErrorCode;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorException;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class TableSchemaUtilTest {

    @Test
    void testGetActualTableSchemaThrowsWhenTargetTableDoesNotExist() {
        Config config =
                ConfigFactory.parseString(
                        "project_id = \"test-project\"\n"
                                + "dataset_id = \"test_dataset\"\n"
                                + "table_id = \"missing_table\"\n");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);

        BigQuery bigQuery = mock(BigQuery.class);
        when(bigQuery.getTable(TableId.of("test-project", "test_dataset", "missing_table")))
                .thenReturn(null);

        try (MockedStatic<BigQueryClientFactory> mockedFactory =
                mockStatic(BigQueryClientFactory.class)) {
            mockedFactory
                    .when(() -> BigQueryClientFactory.getBigQuery(any(ReadonlyConfig.class)))
                    .thenReturn(bigQuery);

            BigQueryConnectorException exception =
                    assertThrows(
                            BigQueryConnectorException.class,
                            () -> TableSchemaUtil.getActualTableSchema(readonlyConfig, false));

            assertEquals(
                    BigQueryConnectorErrorCode.TABLE_NOT_FOUND, exception.getSeaTunnelErrorCode());
        }
    }
}

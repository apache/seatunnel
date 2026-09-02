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
package org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsMessageFormat;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsStartMode;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.exception.AzureEventHubsConnectorException;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class AzureEventHubsSourceTest {

    @Test
    void supportsStreamingWithOrWithoutCheckpointing() {
        AzureEventHubsSource source = source();

        source.setJobContext(
                new JobContext().setJobMode(JobMode.STREAMING).setEnableCheckpoint(true));
        Assertions.assertEquals(Boundedness.UNBOUNDED, source.getBoundedness());

        source.setJobContext(
                new JobContext().setJobMode(JobMode.STREAMING).setEnableCheckpoint(false));
        Assertions.assertEquals(Boundedness.UNBOUNDED, source.getBoundedness());
    }

    @Test
    void rejectsBatchJobs() {
        AzureEventHubsSource source = source();
        source.setJobContext(new JobContext().setJobMode(JobMode.BATCH));

        AzureEventHubsConnectorException exception =
                Assertions.assertThrows(
                        AzureEventHubsConnectorException.class, source::getBoundedness);

        Assertions.assertTrue(exception.getMessage().contains("streaming jobs only"));
    }

    @Test
    void exposesFactoryContract() {
        AzureEventHubsSourceFactory factory = new AzureEventHubsSourceFactory();

        Assertions.assertEquals("AzureEventHubs", factory.factoryIdentifier());
        Assertions.assertEquals(AzureEventHubsSource.class, factory.getSourceClass());
        Assertions.assertNotNull(factory.optionRule());
    }

    @Test
    void sourceIsSerializableForDistributedExecution() {
        CatalogTable catalogTable = CatalogTableUtil.buildSimpleTextTable();
        AzureEventHubsSource source =
                new AzureEventHubsSource(
                        ReadonlyConfig.fromMap(Collections.emptyMap()),
                        config(),
                        catalogTable,
                        new JsonDeserializationSchema(catalogTable, false, false));

        Assertions.assertDoesNotThrow(() -> SerializationUtils.serialize(source));
    }

    private AzureEventHubsSource source() {
        return new AzureEventHubsSource(
                ReadonlyConfig.fromMap(Collections.emptyMap()),
                config(),
                null,
                null,
                ignored -> null);
    }

    private AzureEventHubsSourceConfig config() {
        return AzureEventHubsSourceConfig.builder()
                .connectionString("secret")
                .eventHubName("events")
                .consumerGroup("$Default")
                .startMode(AzureEventHubsStartMode.EARLIEST)
                .format(AzureEventHubsMessageFormat.JSON)
                .fieldDelimiter(",")
                .maxBatchSize(100)
                .pollTimeoutMs(1_000L)
                .prefetchCount(300)
                .build();
    }
}

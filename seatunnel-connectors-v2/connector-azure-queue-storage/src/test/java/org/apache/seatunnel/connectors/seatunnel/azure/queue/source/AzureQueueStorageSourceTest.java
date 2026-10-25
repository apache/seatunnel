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

package org.apache.seatunnel.connectors.seatunnel.azure.queue.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.AuthenticationType;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.AzureQueueSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.MessageEncoding;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.MessageFormat;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.exception.AzureQueueConnectorException;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AzureQueueStorageSourceTest {

    @Test
    void shouldRequireStreamingModeWithCheckpointing() {
        AzureQueueStorageSource source = createSource();

        source.setJobContext(
                new JobContext().setJobMode(JobMode.STREAMING).setEnableCheckpoint(true));
        Assertions.assertEquals(Boundedness.UNBOUNDED, source.getBoundedness());

        source.setJobContext(
                new JobContext().setJobMode(JobMode.STREAMING).setEnableCheckpoint(false));
        AzureQueueConnectorException checkpointException =
                Assertions.assertThrows(AzureQueueConnectorException.class, source::getBoundedness);
        Assertions.assertTrue(checkpointException.getMessage().contains("requires checkpointing"));

        source.setJobContext(new JobContext().setJobMode(JobMode.BATCH).setEnableCheckpoint(true));
        AzureQueueConnectorException batchException =
                Assertions.assertThrows(AzureQueueConnectorException.class, source::getBoundedness);
        Assertions.assertTrue(batchException.getMessage().contains("streaming jobs only"));
    }

    private AzureQueueStorageSource createSource() {
        CatalogTable catalogTable = CatalogTableUtil.buildSimpleTextTable();
        DeserializationSchema<SeaTunnelRow> deserializationSchema =
                new JsonDeserializationSchema(catalogTable, false, false);
        AzureQueueSourceConfig config =
                AzureQueueSourceConfig.builder()
                        .queueName("events")
                        .authenticationType(AuthenticationType.CONNECTION_STRING)
                        .connectionString("UseDevelopmentStorage=true")
                        .format(MessageFormat.JSON)
                        .fieldDelimiter(",")
                        .messageEncoding(MessageEncoding.NONE)
                        .batchSize(32)
                        .visibilityTimeoutSeconds(300)
                        .pollIntervalMillis(1_000)
                        .maxInFlightMessages(1_000)
                        .operationTimeoutMillis(60_000)
                        .build();
        return new AzureQueueStorageSource(config, catalogTable, deserializationSchema);
    }
}

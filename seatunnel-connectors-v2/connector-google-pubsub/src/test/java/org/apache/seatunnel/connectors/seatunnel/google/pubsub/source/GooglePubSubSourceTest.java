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

package org.apache.seatunnel.connectors.seatunnel.google.pubsub.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.config.GooglePubSubSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.config.MessageFormat;
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.exception.GooglePubSubConnectorException;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class GooglePubSubSourceTest {

    @Test
    void shouldRequireStreamingModeWithCheckpointing() {
        GooglePubSubSource source = createSource();

        source.setJobContext(
                new JobContext().setJobMode(JobMode.STREAMING).setEnableCheckpoint(true));
        Assertions.assertEquals(Boundedness.UNBOUNDED, source.getBoundedness());

        source.setJobContext(
                new JobContext().setJobMode(JobMode.STREAMING).setEnableCheckpoint(false));
        GooglePubSubConnectorException checkpointException =
                Assertions.assertThrows(
                        GooglePubSubConnectorException.class, source::getBoundedness);
        Assertions.assertTrue(checkpointException.getMessage().contains("requires checkpointing"));

        source.setJobContext(new JobContext().setJobMode(JobMode.BATCH).setEnableCheckpoint(true));
        GooglePubSubConnectorException batchException =
                Assertions.assertThrows(
                        GooglePubSubConnectorException.class, source::getBoundedness);
        Assertions.assertTrue(batchException.getMessage().contains("streaming jobs only"));
    }

    private GooglePubSubSource createSource() {
        CatalogTable catalogTable = CatalogTableUtil.buildSimpleTextTable();
        DeserializationSchema<SeaTunnelRow> deserializationSchema =
                new JsonDeserializationSchema(catalogTable, false, false);
        GooglePubSubSourceConfig config =
                GooglePubSubSourceConfig.builder()
                        .projectId("project")
                        .subscription("subscription")
                        .format(MessageFormat.JSON)
                        .fieldDelimiter(",")
                        .build();
        return new GooglePubSubSource(config, catalogTable, deserializationSchema);
    }
}

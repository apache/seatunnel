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
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitEnumeratorState;
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.config.GooglePubSubSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.exception.GooglePubSubConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.exception.GooglePubSubConnectorException;

import java.util.Collections;
import java.util.List;

/** Unbounded source for one Google Pub/Sub subscription. */
public class GooglePubSubSource
        implements SeaTunnelSource<SeaTunnelRow, SingleSplit, SingleSplitEnumeratorState> {

    public static final String PLUGIN_NAME = "GooglePubSub";

    private final GooglePubSubSourceConfig config;
    private final CatalogTable catalogTable;
    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;

    private JobContext jobContext;

    public GooglePubSubSource(
            GooglePubSubSourceConfig config,
            CatalogTable catalogTable,
            DeserializationSchema<SeaTunnelRow> deserializationSchema) {
        this.config = config;
        this.catalogTable = catalogTable;
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public Boundedness getBoundedness() {
        if (jobContext != null) {
            if (!JobMode.STREAMING.equals(jobContext.getJobMode())) {
                throw new GooglePubSubConnectorException(
                        GooglePubSubConnectorErrorCode.CONFIGURATION_FAILED,
                        "Google Pub/Sub source supports streaming jobs only");
            }
            if (!jobContext.isEnableCheckpoint()) {
                throw new GooglePubSubConnectorException(
                        GooglePubSubConnectorErrorCode.CONFIGURATION_FAILED,
                        "Google Pub/Sub source requires checkpointing to acknowledge messages");
            }
        }
        return Boundedness.UNBOUNDED;
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public SourceReader<SeaTunnelRow, SingleSplit> createReader(
            SourceReader.Context readerContext) {
        return new GooglePubSubSourceReader(config, deserializationSchema);
    }

    @Override
    public SourceSplitEnumerator<SingleSplit, SingleSplitEnumeratorState> createEnumerator(
            SourceSplitEnumerator.Context<SingleSplit> enumeratorContext) {
        return new SingleSplitEnumerator(enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<SingleSplit, SingleSplitEnumeratorState> restoreEnumerator(
            SourceSplitEnumerator.Context<SingleSplit> enumeratorContext,
            SingleSplitEnumeratorState checkpointState) {
        return new SingleSplitEnumerator(enumeratorContext);
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }
}

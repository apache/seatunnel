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
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.AzureQueueSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.exception.AzureQueueConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.exception.AzureQueueConnectorException;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitEnumeratorState;

import java.util.Collections;
import java.util.List;

/** Unbounded source for one Azure Storage queue. */
public class AzureQueueStorageSource
        implements SeaTunnelSource<SeaTunnelRow, SingleSplit, SingleSplitEnumeratorState> {

    public static final String PLUGIN_NAME = "AzureQueueStorage";

    private final AzureQueueSourceConfig config;
    private final CatalogTable catalogTable;
    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private JobContext jobContext;

    public AzureQueueStorageSource(
            AzureQueueSourceConfig config,
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
                throw new AzureQueueConnectorException(
                        AzureQueueConnectorErrorCode.CONFIGURATION_FAILED,
                        "Azure Queue Storage source supports streaming jobs only");
            }
            if (!jobContext.isEnableCheckpoint()) {
                throw new AzureQueueConnectorException(
                        AzureQueueConnectorErrorCode.CONFIGURATION_FAILED,
                        "Azure Queue Storage source requires checkpointing to delete messages");
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
        return new AzureQueueStorageSourceReader(config, deserializationSchema);
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

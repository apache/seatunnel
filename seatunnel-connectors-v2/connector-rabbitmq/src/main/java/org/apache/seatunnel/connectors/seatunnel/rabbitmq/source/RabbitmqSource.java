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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorException;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitmqSplit;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitmqSplitEnumeratorState;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import java.util.Collections;
import java.util.List;

public class RabbitmqSource
        implements SeaTunnelSource<SeaTunnelRow, RabbitmqSplit, RabbitmqSplitEnumeratorState>,
                SupportParallelism {

    private DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private JobContext jobContext;
    private final RabbitmqConfig rabbitMQConfig;
    private final CatalogTable catalogTable;

    public RabbitmqSource(RabbitmqConfig rabbitMQConfig, CatalogTable catalogTable) {
        this.rabbitMQConfig = rabbitMQConfig;
        this.catalogTable = catalogTable;
        this.deserializationSchema = new JsonDeserializationSchema(catalogTable, false, false);
    }

    @Override
    public Boundedness getBoundedness() {
        if (!JobMode.STREAMING.equals(jobContext.getJobMode())) {
            throw new RabbitmqConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, "not support batch job mode"));
        }
        return rabbitMQConfig.isForE2ETesting() ? Boundedness.BOUNDED : Boundedness.UNBOUNDED;
    }

    @Override
    public String getPluginName() {
        return "RabbitMQ";
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public SourceReader<SeaTunnelRow, RabbitmqSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new RabbitmqSourceReader(deserializationSchema, readerContext, rabbitMQConfig);
    }

    @Override
    public SourceSplitEnumerator<RabbitmqSplit, RabbitmqSplitEnumeratorState> createEnumerator(
            SourceSplitEnumerator.Context<RabbitmqSplit> enumeratorContext) throws Exception {
        return new RabbitmqSplitEnumerator();
    }

    @Override
    public SourceSplitEnumerator<RabbitmqSplit, RabbitmqSplitEnumeratorState> restoreEnumerator(
            SourceSplitEnumerator.Context<RabbitmqSplit> enumeratorContext,
            RabbitmqSplitEnumeratorState checkpointState)
            throws Exception {
        return new RabbitmqSplitEnumerator();
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }
}
